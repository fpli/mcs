package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.adservice.redirect.AdobeRedirectStrategy;
import com.ebay.app.raptor.chocolate.adservice.redirect.RedirectContext;
import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;
import com.ebay.app.raptor.chocolate.adservice.util.DAPResponseHandler;
import com.ebay.app.raptor.chocolate.adservice.util.ParametersParser;
import com.ebay.app.raptor.chocolate.adservice.constant.Errors;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.kernel.util.guid.Guid;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.client.utils.URIBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;

import java.net.UnknownHostException;


/**
 * @author xiangli4
 * Handle ar, impression, email open, and redirection
 */
@Component
@DependsOn("AdserviceService")
public class CollectionService {
  private static final Logger logger = LoggerFactory.getLogger(CollectionService.class);
  private Metrics metrics;
  private static CollectionService instance = null;
  private static final String CHANNEL_ACTION = "channelAction";
  private static final String CHANNEL_TYPE = "channelType";
  private static final String ADOBE_PARTNER_ID = "14";


  @Autowired
  private DAPResponseHandler dapResponseHandler;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  /**
   * Collect impression event and send pixel response
   *
   * @param request raw request
   * @return OK or Error message
   */
  public boolean collectAr(HttpServletRequest request, HttpServletResponse response, CookieReader cookieReader,
                           ContainerRequestContext requestContext) throws Exception {
    dapResponseHandler.sendDAPResponse(request, response, cookieReader, requestContext);
    return true;
  }

  // construct X-EBAY-C-TRACKING header for mcs to send ubi event
  // guid is mandatory, if guid is null, create a guid
  public String constructTrackingHeader(ContainerRequestContext requestContext, CookieReader cookieReader,
                                        String channelType) {
    String cookie = "";
    String rawGuid = cookieReader.getGuid(requestContext);
    if (!StringUtils.isEmpty(rawGuid))
      cookie += "guid=" + rawGuid.substring(0,Constants.GUID_LENGTH);
    else {
      try {
        cookie += "guid=" + new Guid().nextPaddedGUID();
      } catch (UnknownHostException e) {
        logger.warn("Create guid failure: ", e);
        metrics.meter("CreateGuidFailure", 1, Field.of(Constants.CHANNEL_TYPE, channelType));
      }
      logger.warn("No guid");
      metrics.meter("NoGuid", 1, Field.of(Constants.CHANNEL_TYPE, channelType));
    }

    String rawCguid = cookieReader.getCguid(requestContext);
    if (!StringUtils.isEmpty(rawCguid))
      cookie += ",cguid=" + rawCguid.substring(0,Constants.CGUID_LENGTH);
    else {
      logger.warn("No cguid");
      metrics.meter("NoCguid", 1, Field.of(Constants.CHANNEL_TYPE, channelType));
    }

    return cookie;
  }

  /**
   * Collect email entry event(Adobe) and send redirect response
   *
   * @param request raw request
   * @return OK or Error message
   */
  public URI collectRdirect(HttpServletRequest request, HttpServletResponse response,
                            ContainerRequestContext requestContext, CookieReader cookieReader) throws Exception {

    // verify the request
    verifyRedirectRequest(request);

    // get parameters from request
    MultiValueMap<String, String> parameters = ParametersParser.parse(request.getParameterMap());
    String partnerId = parameters.get(Constants.PARTNER_ID).get(0);

    // execute redirect Strategy
    return executeRedirectStrategy(request, response, partnerId, requestContext,cookieReader);
  }

  /**
   * Verify redirect request, if invalid then throw an exception
   */
  private void verifyRedirectRequest(HttpServletRequest request)  throws Exception {
    // no query parameter, rejected
    Map<String, String[]> params = request.getParameterMap();
    if (null == params || params.isEmpty()) {
      logError(Errors.ERROR_REDIRECT_NO_QUERY_PARAMETER);
    }
    MultiValueMap<String, String> parameters = ParametersParser.parse(params);

    // no partnerId, rejected
    if (!parameters.containsKey(Constants.PARTNER_ID) || null == parameters.get(Constants.PARTNER_ID).get(0) ) {
      logError(Errors.ERROR_REDIRECT_NO_PARTNER_ID);
    }

  }

  /**
   * Send redirect response by Strategy Pattern
   */
  private URI executeRedirectStrategy(HttpServletRequest request, HttpServletResponse response, String partnerId, ContainerRequestContext context, CookieReader cookie) throws Exception {
    RedirectContext redirectContext = null;
    switch (partnerId) {
      case ADOBE_PARTNER_ID:
        // AdobeRedirectStrategy has been implemented the Singleton pattern by Enum
        redirectContext = new RedirectContext(AdobeRedirectStrategy.INSTANCE);
        break;
      default:
        logError(Errors.ERROR_REDIRECT_INVALID_PARTNER_ID);
    }
    try {
      return redirectContext.execute(request, response, cookie, context);
    } catch (Exception e) {
      logError(Errors.ERROR_REDIRECT_RUNTIME);
    }
      return new URIBuilder(Constants.DEFAULT_REDIRECT_URL).build();
  }

  /**
   * log error, log metric and throw error with error key
   *
   * @param  error error type
   * @throws Exception exception with error key
   */
  private void logError(String error) throws Exception {
    logger.warn(error);
    metrics.meter(error);
    throw new Exception(error);
  }

}