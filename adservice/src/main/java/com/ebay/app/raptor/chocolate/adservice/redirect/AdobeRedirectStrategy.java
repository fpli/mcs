package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.util.HttpClientConnectionManager;
import com.ebay.app.raptor.chocolate.adservice.util.ParametersParser;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.Errors;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AdobeRedirectStrategy implements RedirectStrategy {
  private HttpClientConnectionManager httpClientConnectionManager;
  private ContainerRequestContext requestContext;
  private static final Logger logger = LoggerFactory.getLogger(AdobeRedirectStrategy.class);
  private Metrics metrics;
  private String redirectSource = "default";

  private static final int REDIRECT_CODE = 301;
  private static final int SUCCESS_CODE = 200;
  private static final String[] TARGET_URL_PARMS = {"mpre", "loc", "url", "URL"};
  private static final String[] ADOBE_PARAMS_LIST = {"id", "ap_visitorId", "ap_category", "ap_deliveryId", "ap_oid"};
  private static final String DEFAULT_REDIRECT_URL = "www.ebay.com";
  private static final String ADOBE_COUNTRY = "country";
  private static final String ADOBE_ID = "id";
  private static final String REDIRECT_URL_SOJ_TAG = "adcamp_landingpage";
  private static final String REDIRECT_SRC_SOJ_TAG = "adcamp_locationsrc";
  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);

  // TODO: REDIRECT_SERVER_DOMAIN need to be defined
  private static final String REDIRECT_SERVER_DOMAIN = "TBD";
  private static HashMap<String, String> ADOBE_COUNTRY_MAP = new HashMap<String, String>();

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

  public AdobeRedirectStrategy(HttpClientConnectionManager connectionManager, ContainerRequestContext context) {
    httpClientConnectionManager = connectionManager;
    requestContext = context;
    this.metrics = ESMetrics.getInstance();
  }

  @Override
  public void process(HttpServletRequest request, HttpServletResponse response) throws IOException {

    // generate Redirect Url
    String redirectUrl = generateRedirectUrl(request);

    // send Response
    response.sendRedirect(redirectUrl);

    // store redirect url and redirect source to Soj
    addAdobeSojTags(redirectUrl, redirectSource);

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
   * @param request
   * @return
   */
  @Override
  public String generateRedirectUrl(HttpServletRequest request) {
    // get loc parameter from request, if loc URL is valid, return it as redirect url
    MultiValueMap<String, String> parameters = ParametersParser.parse(request.getParameterMap());
    // get loc URL
    String targetLocation = getTargetLocation(parameters);
    // if loc URL is valid, return it as redirect url
    if (isValidRedirectUrl(targetLocation)) {
      redirectSource = "loc";
      return targetLocation;
    }

    // get response from Adobe Server, if the url in response is valid, return it as redirect url
    String adobeRedirectUrl = getRedirectUrlByAdobe(request);
    if (isValidRedirectUrl(adobeRedirectUrl)) {
      redirectSource = "adobe";
      return adobeRedirectUrl;
    }

    // generate redirect url by country
    if (parameters.containsKey(ADOBE_COUNTRY)) {
      String county = parameters.get(ADOBE_COUNTRY).get(0);
      if (ADOBE_COUNTRY_MAP.containsKey(county)) {
        redirectSource = "country";
        return ADOBE_COUNTRY_MAP.get(county);
      }
    } else {
      logger.warn("adobe parameter field country is missing: ");
    }

    return DEFAULT_REDIRECT_URL;
  }

  private String getRedirectUrlByAdobe(HttpServletRequest request) {
    CloseableHttpClient client = httpClientConnectionManager.getHttpClient();
    URIBuilder uriBuilder = null;
    HttpContext httpContext = HttpClientContext.create();
    HttpGet httpGet = null;
    String headers = "";
    String redirectUrl = "";
    // generate Adobe Server URL
    try {
      uriBuilder = generateAdobeUrl(request);
    } catch (Exception ex) {
      logger.error("Generate Adobe URL exception", ex);
      return redirectUrl;
    }
    try {
      // call Adobe Server
      httpGet = new HttpGet(uriBuilder.build());
      for (Header header : httpGet.getAllHeaders()) {
        headers = headers + header.toString() + ",";
      }
      // TODO: add timeout
      CloseableHttpResponse response = client.execute(httpGet, httpContext);
      // Get the redirect URL from reponse
      if (isValidResponse(response, httpGet, headers)) {
        redirectUrl = parseRedirectUrl(response);
      }
      response.close();
    } catch (Exception ex) {
      logger.error("Call Adobe Server exception", ex);
      if (null != httpGet) {
        logger.error("AdobeServerException req. URI: " + httpGet.getURI());
      }
      metrics.meter("AdobeServerException");
    }
    // TODO: Will throw an exception when redirectUrl is empty?
    return redirectUrl;
  }

  //TODO
  private String parseRedirectUrl(CloseableHttpResponse response) {
    return "adobe-redirect";
  }

  /**
   * Verify the response and judge is it success ack
   *
   * @param response
   * @param httpGet
   * @param headers
   * @return
   */
  private boolean isValidResponse(CloseableHttpResponse response, HttpGet httpGet, String headers) {
    boolean status = false;
    switch (response.getStatusLine().getStatusCode()) {
      case SUCCESS_CODE: {
        metrics.meter("AdobeServerSuccess");
        status = true;
        break;
      }
      case REDIRECT_CODE: {
        metrics.meter("AdobeServerRedirect");
        logger.error("AdobeServerRedirect req. URI: " + httpGet.getURI() + ", headers: " + headers);
        break;
      }
      default: {
        metrics.meter("AdobeServerFail");
        logger.error("AdobeServerFail req. URI: " + httpGet.getURI() + ", headers: " + headers);
      }
    }
    return status;
  }

  /**
   * Generate the adobe URI,
   * the adobe server get from raptorio config
   * the paramter get from request's parameters
   */
  public URIBuilder generateAdobeUrl(HttpServletRequest request) throws Exception {
    String adobeHost = "";
    URIBuilder uriBuilder = new URIBuilder(adobeHost);
    MultiValueMap<String, String> parameters = ParametersParser.parse(request.getParameterMap());
    // no adobeParams, rejected
    if (!parameters.containsKey(Constants.ADOBE_PARAMS) || parameters.get(Constants.ADOBE_PARAMS).get(0) == null) {
      logger.warn(Errors.ERROR_NO_ADOBE_PARAMS);
      metrics.meter(Errors.ERROR_NO_ADOBE_PARAMS);
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
    return false;
  }

  private void addAdobeSojTags( String redirectUrl, String redirectSource) {
    try {
      // Ubi tracking
      IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      // redirect url
      requestTracker.addTag(REDIRECT_URL_SOJ_TAG, redirectUrl, String.class);

      // redirect source
      requestTracker.addTag(REDIRECT_SRC_SOJ_TAG, redirectSource, String.class);

    } catch (Exception e) {
      logger.warn("Error when tracking ubi for adobe redirect tags", e);
    }

  }
}
