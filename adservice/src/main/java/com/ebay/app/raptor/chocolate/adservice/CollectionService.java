package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.adservice.redirect.AdobeRedirectStrategy;
import com.ebay.app.raptor.chocolate.adservice.redirect.RedirectContext;
import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;
import com.ebay.app.raptor.chocolate.adservice.util.DAPResponseHandler;
import com.ebay.app.raptor.chocolate.adservice.util.ParametersParser;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.Errors;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;


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
  public boolean collectAr(HttpServletRequest request, HttpServletResponse response, CookieReader cookieReader, IEndUserContext endUserContext,
                           ContainerRequestContext requestContext) throws Exception {
    DAPResponseHandler.sendDAPResponse(request, response, cookieReader, endUserContext, requestContext);
    return true;
  }

  /**
   * Parse rotation id from query mkrid
   */
  private long parseRotationId(MultiValueMap<String, String> parameters) {
    long rotationId = -1L;
    if (parameters.containsKey(Constants.MKRID) && parameters.get(Constants.MKRID).get(0) != null) {
      try {
        String rawRotationId = parameters.get(Constants.MKRID).get(0);
        rotationId = Long.valueOf(rawRotationId.replaceAll("-", ""));
      } catch (Exception e) {
        logger.warn(Errors.ERROR_INVALID_MKRID);
        metrics.meter("InvalidMkrid");
      }
    } else {
      logger.warn(Errors.ERROR_NO_MKRID);
      metrics.meter("NoMkrid");
    }

    return rotationId;
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