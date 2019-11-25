package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;
import com.ebay.app.raptor.chocolate.adservice.util.MarketingTrackingEvent;
import com.ebay.app.raptor.chocolate.adservice.util.ParametersParser;
import com.ebay.app.raptor.chocolate.adservice.util.RedirectionEvent;
import com.ebay.app.raptor.chocolate.jdbc.data.LookupManager;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.util.guid.Guid;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Response;
import java.net.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common Redirection Strategy
 *
 * Created by jialili1 on 11/18/19
 */
abstract public class BaseRedirectStrategy implements RedirectStrategy {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  public Metrics metrics;
  public RedirectionEvent redirectionEvent;

  // TODO: REDIRECT_SERVER_DOMAIN need to be defined
  private static final String REDIRECT_SERVER_DOMAIN = "TBD";
  private static final String[] TARGET_URL_PARMS = {"mpre", "loc", "url", "URL"};
  private static final String MCS_SERVICE_NAME = "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46";
  private static final String DEFAULT_REDIRECT_URL = "http://www.ebay.com";
  private static final String REDIRECT_URL_SOJ_TAG = "adcamp_landingpage";
  private static final String REDIRECT_SRC_SOJ_TAG = "adcamp_locationsrc";

  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);

  BaseRedirectStrategy() {
    this.metrics = ESMetrics.getInstance();
  }

  @Override
  public URI process(HttpServletRequest request, HttpServletResponse response, CookieReader cookie,
  ContainerRequestContext context) throws URISyntaxException {
    MultiValueMap<String, String> parameters = ParametersParser.parse(request.getParameterMap());

    redirectionEvent = new RedirectionEvent(getParam(parameters, Constants.MKCID), getParam(parameters, Constants.MKEVT),
        getParam(parameters, Constants.PARTNER_ID));

    // generate Redirect Url
    generateRedirectUrl(parameters);

    // call mcs to send ubi event if redirect url is not an ebay doamin
    callMcs(request, cookie, context, parameters);

    return new URIBuilder(redirectionEvent.getRedirectUrl()).build();
  }

  /**
   * Verify the redirect url, if loc URL is ebay url or in whitelist return true
   * Must avoid infinite redirects
   */
  public boolean isValidRedirectUrl(String url) {
    // avoid infinite redirects
    URL urlObj;
    try {
      urlObj = new URL(url);
    } catch (MalformedURLException e) {
      logger.warn("Redirect URL is wrong: ", e);
      return false;
    }
    String requestUri = urlObj.getPath();
    if (!StringUtils.isEmpty(requestUri)) {
      String[] tokens = requestUri.split("/");
      // If the URL's domain is redirect host, and the api is redirect
      // we identified it's a redirect URL in chocolate, need ignore it
      if (tokens.length > 1 && tokens[0].equalsIgnoreCase(REDIRECT_SERVER_DOMAIN)
          && tokens[3].equals(Constants.REDIRECT)) {
        return false;
      }
    }

    // judge whether the url contain ebay domain
    Matcher m = ebaysites.matcher(url.toLowerCase());
    if (m.find()) {
      return true;
    }

    return isWhiteListDomain(url);
  }

  protected void generateRedirectUrl(MultiValueMap<String, String> parameters) {
    // The method should have specific implementation
  }

  /**
   * Judge whether the redirection url is a valid thirdparty url
   */
  private boolean isWhiteListDomain(String redirectUrl) {
    return LookupManager.isApprovedOffEbayDestination(redirectUrl);
  }

  /**
   * Get the target location URL from request, the priority is mpre > loc > url > URL
   */
  public String getTargetLocation(MultiValueMap<String, String> parameters) {
    String result = null;
    for (String targetUrlParm : TARGET_URL_PARMS) {
      // get target location
      if (parameters.containsKey(targetUrlParm)) {
        result = parameters.get(targetUrlParm).get(0);
      }
      if (!StringUtils.isEmpty(result)) {
        return result;
      }
    }
    return null;
  }

  /**
   * Generate a mcs click event and call mcs
   */
  private void callMcs(HttpServletRequest request, CookieReader cookie, ContainerRequestContext context,
                      MultiValueMap<String, String> parameters) throws URISyntaxException{

    Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient", MCS_SERVICE_NAME);
    Client mktClient = GingerClientBuilder.newClient(config);
    String mcsEndpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    // build mcs target url, add mkcid and mkevt
    URIBuilder uriBuilder = new URIBuilder(redirectionEvent.getRedirectUrl());
    uriBuilder.addParameter(Constants.MKCID, redirectionEvent.getChannelId())
        .addParameter(Constants.MKEVT, redirectionEvent.getActionId());

    // Adobe needs additional parameters
    if ("adobe".equals(redirectionEvent.getPartner())) {
      uriBuilder.addParameter(Constants.PARTNER_ID, redirectionEvent.getPartnerId())
          .addParameter(REDIRECT_URL_SOJ_TAG, redirectionEvent.getRedirectUrl())
          .addParameter(REDIRECT_SRC_SOJ_TAG, redirectionEvent.getRedirectSource());
    }

    // generate marketing event
    MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
    mktEvent.setTargetUrl(uriBuilder.toString());
    mktEvent.setReferrer(request.getHeader("Referer"));

    // add all headers except Cookie
    Invocation.Builder builder = mktClient.target(mcsEndpoint).path("/events").request();

    // add Commerce-OS standard header
    builder = builder.header("X-EBAY-C-ENDUSERCTX", constructEndUserContextHeader(request))
        .header("X-EBAY-C-TRACKING", constructTrackingHeader(cookie, context));

    // call MCS
    Response ress = builder.post(Entity.json(mktEvent));
    ress.close();
  }

  /**
   * Construct X-EBAY-C-TRACKING header with guid and cguid
   */
  public String constructTrackingHeader(CookieReader cookie, ContainerRequestContext context) {
    String cguid = cookie.getCguid(context);
    if (!StringUtils.isEmpty(cguid)) {
      cguid = cguid.substring(0, Constants.CGUID_LENGTH);
    } else {
      try {
        cguid = new Guid().nextPaddedGUID();
      } catch (UnknownHostException e) {
        logger.warn("Create Cguid failure: ", e);
        metrics.meter("CreateCGuidFailure", 1, Field.of(Constants.CHANNEL_TYPE, redirectionEvent.getChannelType()));
      }
      logger.warn("No cguid");
      metrics.meter("NoCguid", 1, Field.of(Constants.CHANNEL_TYPE, redirectionEvent.getChannelType()));
    }

    String guid = cookie.getGuid(context);
    if (!StringUtils.isEmpty(guid))
      guid = guid.substring(0, Constants.CGUID_LENGTH);
    else {
      try {
        guid = new Guid().nextPaddedGUID();
      } catch (UnknownHostException e) {
        logger.warn("Create guid failure: ", e);
        metrics.meter("CreateGuidFailure", 1, Field.of(Constants.CHANNEL_TYPE, redirectionEvent.getChannelType()));
      }
      logger.warn("No guid");
      metrics.meter("NoGuid", 1, Field.of(Constants.CHANNEL_TYPE, redirectionEvent.getChannelType()));
    }

    return "guid=" + guid + "," + "cguid=" + cguid;
  }

  /**
   * Construct X-EBAY-C-ENDUSERCTX header with user agent
   */
  public String constructEndUserContextHeader(HttpServletRequest request) {
    String userAgent = request.getHeader("User-Agent");

    return "userAgent=" + userAgent;
  }

  private String getParam(MultiValueMap<String, String> parameters, String param) {
    if (parameters.containsKey(param) && parameters.get(param) != null) {
      return parameters.get(param).get(0);
    }
    else
      return null;
  }

}
