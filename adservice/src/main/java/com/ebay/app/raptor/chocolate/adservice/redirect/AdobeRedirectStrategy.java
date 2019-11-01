package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;
import com.ebay.app.raptor.chocolate.adservice.util.MarketingTrackingEvent;
import com.ebay.app.raptor.chocolate.adservice.util.ParametersParser;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.Errors;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.client.utils.URIBuilder;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Singleton pattern
public enum AdobeRedirectStrategy implements RedirectStrategy {
  // Implement the Singleton pattern using Enum, this solution is thread safety
  INSTANCE;
  private static final Logger logger = LoggerFactory.getLogger(AdobeRedirectStrategy.class);
  private Metrics metrics;

  private static final int REDIRECT_CODE = 3;
  private static final int SUCCESS_CODE = 2;
  private static final String[] TARGET_URL_PARMS = {"mpre", "loc", "url", "URL"};
  private static final String[] ADOBE_PARAMS_LIST = {"id", "ap_visitorId", "ap_category", "ap_deliveryId", "ap_oid", "data"};
  private static final String DEFAULT_REDIRECT_URL = "http://www.ebay.com";
  private static final String ADOBE_COUNTRY = "country";
  private static final String ADOBE_ID = "id";
  private static final String REDIRECT_SOURCE = "redirect_source";
  private static final String TARGET_URL = "target_url";
  private static final String REDIRECT_URL_SOJ_TAG = "adcamp_landingpage";
  private static final String REDIRECT_SRC_SOJ_TAG = "adcamp_locationsrc";
  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);
  private static final String MCS_SERVICE_NAME = "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46";
  private static final String ADSERVICE_SERVICE_NAME = "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46";
  private static final int HUNDRED = 100;


  // TODO: REDIRECT_SERVER_DOMAIN need to be defined
  private static final String REDIRECT_SERVER_DOMAIN = "TBD";
  private static HashMap<String, String> ADOBE_COUNTRY_MAP = new HashMap<String, String>();

  @Context
  private HttpServletRequest servletRequest;

  static {
    ADOBE_COUNTRY_MAP.put("AT", "http://www.ebay.at");
    ADOBE_COUNTRY_MAP.put("AU", "http://www.ebay.com.au");
    ADOBE_COUNTRY_MAP.put("BEFR", "http://www.befr.ebay.be");
    ADOBE_COUNTRY_MAP.put("BENL", "http://www.benl.ebay.be");
    ADOBE_COUNTRY_MAP.put("CA", "http://www.ebay.ca");
    ADOBE_COUNTRY_MAP.put("CAFR", "http://www.cafr.ebay.ca");
    ADOBE_COUNTRY_MAP.put("CH", "http://www.ebay.ch");
    ADOBE_COUNTRY_MAP.put("CN", "http://www.ebay.cn");
    ADOBE_COUNTRY_MAP.put("DE", "http://www.ebay.de");
    ADOBE_COUNTRY_MAP.put("ES", "http://www.ebay.es");
    ADOBE_COUNTRY_MAP.put("FR", "http://www.ebay.fr");
    ADOBE_COUNTRY_MAP.put("HK", "http://www.ebay.com.hk");
    ADOBE_COUNTRY_MAP.put("IE", "http://www.ebay.ie");
    ADOBE_COUNTRY_MAP.put("IN", "http://www.ebay.in");
    ADOBE_COUNTRY_MAP.put("IT", "http://www.ebay.it");
    ADOBE_COUNTRY_MAP.put("MOTORS", "http://www.motors.ebay.com");
    ADOBE_COUNTRY_MAP.put("MY", "http://www.ebay.com.my");
    ADOBE_COUNTRY_MAP.put("NL", "http://www.ebay.nl");
    ADOBE_COUNTRY_MAP.put("PH", "http://www.ebay.ph");
    ADOBE_COUNTRY_MAP.put("PL", "http://www.ebay.pl");
    ADOBE_COUNTRY_MAP.put("SG", "http://www.ebay.com.sg");
    ADOBE_COUNTRY_MAP.put("UK", "http://www.ebay.co.uk");
    ADOBE_COUNTRY_MAP.put("US", "http://www.ebay.com");
    ADOBE_COUNTRY_MAP.put("VN", "http://www.ebay.vn");

  }

  AdobeRedirectStrategy() {
    this.metrics = ESMetrics.getInstance();
  }

  @Override
  public URI process(HttpServletRequest request, HttpServletResponse response, CookieReader cookie, ContainerRequestContext context) throws IOException, URISyntaxException {

    MultiValueMap<String, String> parameters = ParametersParser.parse(request.getParameterMap());

    // generate Redirect Url
    Map<String, String> redirectPair = generateRedirectUrl(parameters);
    String redirectUrl = redirectPair.get(TARGET_URL);
    String redirectSource = redirectPair.get(REDIRECT_SOURCE);

    // Store data to Soj by MCS, generate a click event to call MCS
    Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient", MCS_SERVICE_NAME);
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);
    // Generate marketing event
    MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
    String sojTags = request.getParameter(Constants.SOJ_TAGS);
    String redirectUrlParam = REDIRECT_URL_SOJ_TAG + "=" + redirectUrl;
    String redirectSourceParam = REDIRECT_SRC_SOJ_TAG + "=" + redirectSource;
    String targetUrl = String.format("http://www.ebay.com?mkevt=1&mkcid=8&mkrid=222&%s&%s&%s", sojTags, redirectUrlParam, redirectSourceParam);
    mktEvent.setTargetUrl(targetUrl);

    // cookie and userAgent
    String cguid = cookie.getCguid(context);
    if (!StringUtils.isEmpty(cguid)) {
      cguid = cguid.substring(0, 31);
    }
    String guid = cookie.getGuid(context);
    if (!StringUtils.isEmpty(cguid)) {
      guid = guid.substring(0, 31);
    }
    String userAgent = request.getHeader("User-Agent");
    if(StringUtils.isEmpty(userAgent)) {
      userAgent = "userAgent=ebayUserAgent/eBayIOS;5.28.0;iOS;12.1.2;Apple;iPhone11_2;AT&T;375x812;3.0";
    }

    // call MCS
    Response ress = mktClient.target(endpoint).path("/events").request()
        .header("Content-Type", "application/json")
        .header("X-EBAY-C-ENDUSERCTX", userAgent)
        .header("X-EBAY-C-TRACKING", "guid=" + guid + "," + "cguid=" + cguid)
        .post(Entity.json(mktEvent));
    ress.close();
    return new URIBuilder(redirectUrl).build();
  }

  /**
   * TODO: Verify the redirect url, if loc URL is ebay url or in whitelist return true
   * Must avoid infinite redirects!
   *
   * @param redirectUrl
   * @return
   */
  @Override
  public boolean isValidRedirectUrl(String redirectUrl) {
    // avoid infinite redirects
    URL urlObj = null;
    try {
      urlObj = new URL(redirectUrl);
    } catch (MalformedURLException e) {
      logger.warn("Redirect URL is wrong: ");
      return false;
    }
    String requestUri = urlObj.getPath();
    if (!StringUtils.isEmpty(requestUri)) {
      String[] tokens = requestUri.split("/");
      // If the URL's domain is redirect host, and the api is redirect
      // we identified it's a redirect URL in chocolate, need ignore it
      if (tokens.length > 1 && tokens[0].equalsIgnoreCase(REDIRECT_SERVER_DOMAIN) && tokens[3].equals(Constants.REDIRECT)) {
        return false;
      }
    }

    // judge whether the url contain ebay domain
    Matcher m = ebaysites.matcher(redirectUrl.toLowerCase());
    if(m.find()) {
      return true;
    }

    // TODO judge whether the url contain whitelist domain
    return isWhiteListDomain(redirectUrl);
  }

  /**
   * Generate the redirect URL
   *
   * @param parameters
   * @return
   */
  public Map<String, String> generateRedirectUrl(MultiValueMap<String, String> parameters) {
    Map<String, String> result = new HashMap<String, String>();
    result.put(REDIRECT_SOURCE, "default");
    result.put(TARGET_URL, DEFAULT_REDIRECT_URL);
    // get loc parameter from request, if loc URL is valid, return it as redirect url
    // get loc URL
    String targetLocation = getTargetLocation(parameters);
    // if loc URL is valid, return it as redirect url
    if (isValidRedirectUrl(targetLocation)) {
      result.put(REDIRECT_SOURCE, "loc");
      result.put(TARGET_URL, targetLocation);
      return result;
    }

    // get response from Adobe Server, if the url in response is valid, return it as redirect url
    String adobeRedirectUrl = getRedirectUrlByAdobe(parameters);
    if (isValidRedirectUrl(adobeRedirectUrl)) {
      result.put(REDIRECT_SOURCE, "adobe");
      result.put(TARGET_URL, targetLocation);
      return result;
    }

    // generate redirect url by country
    if (parameters.containsKey(ADOBE_COUNTRY)) {
      String county = parameters.get(ADOBE_COUNTRY).get(0);
      if (ADOBE_COUNTRY_MAP.containsKey(county)) {
        result.put(REDIRECT_SOURCE, "country");
        result.put(TARGET_URL, targetLocation);
        return result;
      }
    } else {
      logger.warn("adobe parameter field country is missing: ");
    }

    return result;
  }

  private String getRedirectUrlByAdobe(MultiValueMap<String, String> parameters) {
    // generate Adobe Server URL
    URIBuilder uriBuilder = null;
    String redirectUrl = "";
    // get the adobe service info from application.properties in resources dir
    Configuration config = ConfigurationBuilder.newConfig("adservice.mktAdobeClient");
    Client mktClient = GingerClientBuilder.newClient(config);
    Map<String, Object> aa = mktClient.getConfiguration().getProperties();
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);
      try {
        uriBuilder = generateAdobeUrl(parameters, endpoint);
        // Init the webTarget instance and set the property FOLLOW_REDIRECTS
        // FOLLOW_REDIRECTS = false means get method will not auto connect the redirect URL in 301 response
        WebTarget webTarget = mktClient.target(uriBuilder.build()).property(ClientProperties.FOLLOW_REDIRECTS, Boolean.FALSE);
        Response response = webTarget.request().get();

        // Get the redirect URL from reponse
        if (isValidResponse(response, uriBuilder)) {
          redirectUrl = parseRedirectUrl(response);
        }
        response.close();
    } catch (Exception ex) {
      logger.error("Generate Redirect URL from Adobe exception", ex);
      metrics.meter("AdobeServerException");
      return redirectUrl;
    }

    // TODO: Will throw an exception when redirectUrl is empty?
    return redirectUrl;
  }

  //TODO
  private String parseRedirectUrl(Response response) {

    return response.getLocation().toString();
  }

  /**
   * Verify the response and judge is it success ack
   *
   * @param response
   * @param uriBuilder
   * @return
   */
  private boolean isValidResponse(Response response, URIBuilder uriBuilder) throws URISyntaxException {
    boolean result = false;
    int statusHeadCode = response.getStatus() / HUNDRED;

    switch (statusHeadCode) {
      case REDIRECT_CODE: {
        metrics.meter("AdobeServerRedirect");
        logger.error("AdobeServerRedirect req. URI: " + uriBuilder.build());
        result = true;
        break;
      }
      default: {
        metrics.meter("AdobeServerFail");
        logger.error("AdobeServerFail req. URI: " + uriBuilder.build());
      }
    }
    return result;
  }

  /**
   * Generate the adobe URI,
   * the adobe server get from raptorio config
   * the paramter get from request's parameters
   */
  public URIBuilder generateAdobeUrl(MultiValueMap<String, String> parameters, String endpoint) throws Exception {
    URIBuilder uriBuilder = new URIBuilder(endpoint);
    // if the url has no adobeParams, we will construct the url with the possible list of params
    if (!parameters.containsKey(Constants.ADOBE_PARAMS) || parameters.get(Constants.ADOBE_PARAMS).get(0) == null) {
      logger.warn(Errors.ERROR_REDIRECT_NO_ADOBE_PARAMS);
      metrics.meter(Errors.ERROR_REDIRECT_NO_ADOBE_PARAMS);
      // construct the url with the possible list of params,  only "id" in the the params list is Mandatory
      for (String adobeParam : ADOBE_PARAMS_LIST) {
        if (parameters.containsKey(adobeParam)) {
          uriBuilder.addParameter(adobeParam, parameters.get(adobeParam).get(0));
        } else {
          // Log the warning info when "id" is miss
          if (adobeParam.equalsIgnoreCase(ADOBE_ID)) {
            logger.warn("adobe Mandatory parameter field id is missing: ");
          }
        }
      }
      return uriBuilder;
    }
    String[] adobeParams = URLDecoder.decode(parameters.get(Constants.ADOBE_PARAMS).get(0), "UTF-8").split(",");
    // check the value of adobeParams, it must be a subset of parameters in request
    for (String adobeParam : adobeParams) {
      if (parameters.containsKey(adobeParam)) {
        uriBuilder.addParameter(adobeParam, parameters.get(adobeParam).get(0));
      } else {
        logger.warn("adobeParams field has wrong parameter name: " + adobeParam);
      }
    }
    return uriBuilder;
  }


  /**
   * Get the target location URL from request, the priority is mpre > loc > url > URL
   * the paramter get from request's parameters
   */
  private String getTargetLocation(MultiValueMap<String, String> parameters) {
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
   * TODO
   */
  private boolean isWhiteListDomain(String redirectUrl) {
    // judge whether the url contain whitelist domain
    return true;
  }


}
