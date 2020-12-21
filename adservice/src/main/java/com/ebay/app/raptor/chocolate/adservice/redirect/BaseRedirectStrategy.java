package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.EmailPartnerIdEnum;
import com.ebay.app.raptor.chocolate.adservice.util.MCSCallback;
import com.ebay.app.raptor.chocolate.adservice.util.MarketingTrackingEvent;
import com.ebay.app.raptor.chocolate.adservice.util.ParametersParser;
import com.ebay.app.raptor.chocolate.jdbc.data.LookupManager;
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
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.container.ContainerRequestContext;
import java.net.*;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
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

  private static final String REDIRECT_SERVER_DOMAIN = "www.ebayadservices.com";
  private static final String REDIRECT_URL_SOJ_TAG = "adcamp_landingpage";
  private static final String REDIRECT_SRC_SOJ_TAG = "adcamp_locationsrc";
  private static final int REDIRECT_API_OFFSET = 3;

  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);

  public BaseRedirectStrategy() {
    this.metrics = ESMetrics.getInstance();
  }

  @Override
  public URI process(HttpServletRequest request, ContainerRequestContext context, Client mktClient, String endpoint)
      throws URISyntaxException {
    MultiValueMap<String, String> parameters = ParametersParser.parse(request.getParameterMap());

    // build redirection event
    redirectionEvent = new RedirectionEvent(getParam(parameters, Constants.MKCID),
        getParam(parameters, Constants.MKEVT), getParam(parameters, Constants.MKPID));

    long startTime = startTimerAndLogData(Field.of(Constants.CHANNEL_TYPE, redirectionEvent.getChannelType()),
        Field.of(Constants.PARTNER, redirectionEvent.getPartner()));

    // generate Redirect Url
    generateRedirectUrl(parameters);

    // check if target url is ebay domain
    checkEbayDomain(redirectionEvent.getRedirectUrl(), parameters);

    if (!redirectionEvent.getIsEbayDomain()) {
      callMcs(request, parameters, mktClient, endpoint);
    }

    stopTimerAndLogData(startTime, Field.of(Constants.CHANNEL_TYPE, redirectionEvent.getChannelType()),
      Field.of(Constants.PARTNER, redirectionEvent.getPartner()));

    return new URIBuilder(redirectionEvent.getRedirectUrl()).build();
  }

  /**
   * Verify the redirect url, if it is an ebay url or in whitelist, then return true
   * Must avoid infinite redirects
   */
  public boolean isValidRedirectUrl(String redirectUrl) {
    // empty landing page
    if (StringUtils.isEmpty(redirectUrl)) {
      logger.warn("Redirect URL is empty " );
      return false;
    }

    // ebay domain
    Matcher m = ebaysites.matcher(redirectUrl.toLowerCase());
    if (m.find()) {
      return true;
    }

    // Valid thirdparty domain
    if (LookupManager.isApprovedOffEbayDestination(redirectUrl))
      return true;

    // avoid infinite redirect
    URL urlObj;
    try {
      urlObj = new URL(redirectUrl);
    } catch (MalformedURLException e) {
      logger.warn("Error occurred when malformed URL, Redirect URL is wrong!");
      return false;
    }
    if (REDIRECT_SERVER_DOMAIN.equals(urlObj.getHost())) {
      return false;
    }

    return false;
  }

  /**
   * Get the target location URL from request, the priority is mpre > loc > url > URL
   */
  public String getTargetLocation(MultiValueMap<String, String> parameters) {
    String result = null;
    for (String targetUrlParm : Constants.getTargetUrlParms()) {
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
  private void callMcs(HttpServletRequest request, MultiValueMap<String, String> parameters, Client mktClient,
                       String endpoint) throws URISyntaxException{
    // build mcs target url, add all original parameter for ubi events except target url parameter
    URIBuilder uriBuilder = new URIBuilder(redirectionEvent.getRedirectUrl());
    parameters.forEach((key, val) -> {
      if (!Arrays.asList(Constants.getTargetUrlParms()).contains(key)) {
        uriBuilder.addParameter(key, val.get(0));
      }
    });

    // Adobe needs additional parameters
    addAdobeParams(uriBuilder);

    // generate marketing event
    MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
    mktEvent.setTargetUrl(uriBuilder.toString());
    mktEvent.setReferrer(request.getHeader(Constants.REFERER));

    Invocation.Builder builder = mktClient.target(endpoint).path("/events").request();

    // add all headers
    final Enumeration<String> headers = request.getHeaderNames();
    while (headers.hasMoreElements()) {
      String header = headers.nextElement();
      String values = request.getHeader(header);
      builder = builder.header(header, values);
    }

    // add Commerce-OS standard header
    builder = builder.header("X-EBAY-C-ENDUSERCTX", constructEndUserContextHeader(request))
        .header("X-EBAY-C-TRACKING", constructCookieHeader());

    // call MCS
    builder.async().post(Entity.json(mktEvent), new MCSCallback());
  }

  /**
   * If the target url is ebay domain, then don't call mcs and add all parameters to target url
   * Let the landing page send click events to mcs
   */
  private void checkEbayDomain(String redirectUrl, MultiValueMap<String, String> parameters) {
    // 3rd party
    if (!ebaysites.matcher(redirectUrl.toLowerCase()).find()) {
      redirectionEvent.setIsEbayDomain(false);
    }
    // ebay page
    else {
      redirectionEvent.setIsEbayDomain(true);

      // add all parameters except landing page parameter to the target url
      try {
        URIBuilder uriBuilder = new URIBuilder(redirectUrl);

        Set<String> keySet = parameters.keySet();
        for (String key : keySet) {
          if (Arrays.asList(Constants.getTargetUrlParms()).contains(key)) {
            continue;
          }
          uriBuilder.addParameter(key, parameters.getFirst(key));
        }

        // Adobe need additional params
        addAdobeParams(uriBuilder);

        redirectionEvent.setRedirectUrl(uriBuilder.build().toString());
      } catch (URISyntaxException e) {
        logger.warn("Build redirect url fail", e);
        redirectionEvent.setIsEbayDomain(false);
      }
    }
  }

  /**
   * Construct X-EBAY-C-TRACKING header with guid
   */
  private String constructCookieHeader() {
    String guid = "";
    try {
      guid = new Guid().nextPaddedGUID();
    } catch (UnknownHostException e) {
      logger.warn("Create guid failure: ", e);
      metrics.meter("CreateGuidFailure", 1, Field.of(Constants.CHANNEL_TYPE, redirectionEvent.getChannelType()));
    }
    return "guid=" + guid;
  }

  /**
   * Construct X-EBAY-C-ENDUSERCTX header with user agent
   */
  private String constructEndUserContextHeader(HttpServletRequest request) {
    String userAgent = request.getHeader(Constants.USER_AGENT);

    return "userAgent=" + userAgent;
  }

  /**
   * Get parameter from url
   */
  private String getParam(MultiValueMap<String, String> parameters, String param) {
    if (parameters.containsKey(param) && parameters.get(param) != null) {
      return parameters.get(param).get(0);
    }
    else
      return null;
  }

  /**
   * Adobe need to add adcamp_landingpage and adcamp_locationsrc
   */
  private void addAdobeParams(URIBuilder uriBuilder) {
    if (EmailPartnerIdEnum.ADOBE.getPartner().equals(redirectionEvent.getPartner())) {
      uriBuilder.addParameter(REDIRECT_URL_SOJ_TAG, redirectionEvent.getRedirectUrl())
          .addParameter(REDIRECT_SRC_SOJ_TAG, redirectionEvent.getRedirectSource());
    }
  }

  /**
   * Starts the timer and logs some basic info
   *
   * @param additionalFields channelType, partner
   * @return start time
   */
  private long startTimerAndLogData(Field<String, Object>... additionalFields) {
    long startTime = System.currentTimeMillis();
    logger.debug(String.format("StartTime: %d", startTime));
    metrics.meter("RedirectionInput", 1, startTime, additionalFields);
    return startTime;
  }

  /**
   * Stops the timer and logs relevant debugging messages
   *
   * @param startTime        the start time, so that latency can be calculated
   * @param additionalFields channelType, partner
   */
  private void stopTimerAndLogData(long startTime, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    metrics.meter("RedirectionSuccess", 1, endTime, additionalFields);
    metrics.mean("RedirectionLatency", endTime - startTime);
  }

}
