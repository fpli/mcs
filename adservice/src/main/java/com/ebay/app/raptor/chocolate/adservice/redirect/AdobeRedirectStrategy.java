package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.EmailPartnerIdEnum;
import com.ebay.app.raptor.chocolate.adservice.constant.Errors;
import com.ebay.app.raptor.chocolate.adservice.util.MonitorUtil;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.lang.management.MonitorInfo;
import java.net.*;
import java.util.HashMap;

/**
 * For Adobe redirection cases
 */
public class AdobeRedirectStrategy extends BaseRedirectStrategy {
  private static final Logger logger = LoggerFactory.getLogger(AdobeRedirectStrategy.class);

  private static final int REDIRECT_CODE = 3;
  private static final int SUCCESS_CODE = 2;
  private static final String[] ADOBE_PARAMS_LIST = {"id", "ap_visitorId", "ap_category", "ap_deliveryId",
      "ap_oid", "data"};
  private static final String ADOBE_COUNTRY = "country";
  private static final String ADOBE_ID = "id";
  private static final int HUNDRED = 100;

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

  /**
   * Generate the redirect URL
   */
  @Override
  public void generateRedirectUrl(MultiValueMap<String, String> parameters) {
    redirectionEvent.setRedirectSource("default");
    redirectionEvent.setRedirectUrl(ApplicationOptions.getInstance().getRedirectHomepage());
    // get loc parameter from request, if loc URL is valid, return it as redirect url
    // get loc URL
    String targetLocation = getTargetLocation(parameters);
    // if loc URL is valid, return it as redirect url
    if (!StringUtils.isEmpty(targetLocation) && isValidRedirectUrl(targetLocation)) {
      redirectionEvent.setRedirectSource("loc");
      redirectionEvent.setRedirectUrl(targetLocation);
      return;
    }

    // get response from Adobe Server, if the url in response is valid, return it as redirect url
    String adobeRedirectUrl = getRedirectUrlByAdobe(parameters);
    if (isValidRedirectUrl(adobeRedirectUrl)) {
      redirectionEvent.setRedirectSource(EmailPartnerIdEnum.ADOBE.getPartner());
      redirectionEvent.setRedirectUrl(adobeRedirectUrl);
      return;
    }

    // generate redirect url by country
    if (parameters.containsKey(ADOBE_COUNTRY)) {
      String county = parameters.get(ADOBE_COUNTRY).get(0);
      if (ADOBE_COUNTRY_MAP.containsKey(county)) {
        redirectionEvent.setRedirectSource("country");
        redirectionEvent.setRedirectUrl(ADOBE_COUNTRY_MAP.get(county));
      }
    } else {
      logger.warn(Errors.REDIRECT_NO_ADOBE_COUNTRY);
      MonitorUtil.info(Errors.REDIRECT_NO_ADOBE_COUNTRY);
    }
  }

  private String getRedirectUrlByAdobe(MultiValueMap<String, String> parameters) {
    // generate Adobe Server URL
    URIBuilder uriBuilder = null;
    String redirectUrl = "";
    // get the adobe service info from application.properties in resources dir
    Configuration config = ConfigurationBuilder.newConfig("adservice.mktAdobeClient");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);
    try {
      uriBuilder = generateAdobeUrl(parameters, endpoint);
      // Init the webTarget instance and set the property FOLLOW_REDIRECTS
      // FOLLOW_REDIRECTS = false means get method will not auto connect the redirect URL in 301 response
      WebTarget webTarget = mktClient.target(uriBuilder.build())
          .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.FALSE);
      Response response = webTarget.request().get();

      // Get the redirect URL from reponse
      if (isValidResponse(response, uriBuilder)) {
        redirectUrl = parseRedirectUrl(response);
      }
      response.close();
    } catch (Exception ex) {
      logger.error("Generate Redirect URL from Adobe exception", ex);
      MonitorUtil.info("AdobeServerException");
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
   * Verify the response and judge if it is a success ack
   */
  private boolean isValidResponse(Response response, URIBuilder uriBuilder) throws URISyntaxException {
    int statusHeadCode = response.getStatus() / HUNDRED;

    if (REDIRECT_CODE == statusHeadCode) {
      MonitorUtil.info("AdobeServerRedirect");
      logger.info("AdobeServerRedirect req. URI: " + uriBuilder.build());
      return true;
    } else {
      MonitorUtil.info("AdobeServerFail");
      logger.error("AdobeServerFail req. URI: " + uriBuilder.build());
      return false;
    }
  }

  /**
   * Generate the adobe URI,
   * the adobe server get from raptorio config
   * the paramter get from request's parameters
   */
  public URIBuilder generateAdobeUrl(MultiValueMap<String, String> parameters, String endpoint)
      throws URISyntaxException, UnsupportedEncodingException {
    URIBuilder uriBuilder = new URIBuilder(endpoint);

    // id is mandatory
    if (!parameters.containsKey(ADOBE_ID) || parameters.getFirst(ADOBE_ID) == null) {
      logger.warn(Errors.REDIRECT_NO_ADOBE_ID);
      MonitorUtil.info(Errors.REDIRECT_NO_ADOBE_ID);
    }

    // if the url has no adobeParams, we will construct the url with the possible list of params
    if (!parameters.containsKey(Constants.ADOBE_PARAMS) || parameters.get(Constants.ADOBE_PARAMS).get(0) == null) {
      logger.warn(Errors.REDIRECT_NO_ADOBE_PARAMS);
      MonitorUtil.info(Errors.REDIRECT_NO_ADOBE_PARAMS);
      // construct the url with the possible list of params,  only "id" in the the params list is Mandatory
      for (String adobeParam : ADOBE_PARAMS_LIST) {
        if (parameters.containsKey(adobeParam)) {
          uriBuilder.addParameter(adobeParam, parameters.get(adobeParam).get(0));
        }
      }

      return uriBuilder;
    }

    String[] adobeParams = URLDecoder.decode(parameters.get(Constants.ADOBE_PARAMS).get(0), "UTF-8").split(",");
    // check the value of adobeParams, it must be a subset of parameters in request
    for (String adobeParam : adobeParams) {
      if (parameters.containsKey(adobeParam)) {
        uriBuilder.addParameter(adobeParam, parameters.get(adobeParam).get(0));
      }
    }
    return uriBuilder;
  }

}
