package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.adservice.redirect.AdobeRedirectStrategy;
import com.ebay.app.raptor.chocolate.adservice.redirect.RedirectContext;
import com.ebay.app.raptor.chocolate.adservice.redirect.ThirdpartyRedirectStrategy;
import com.ebay.app.raptor.chocolate.adservice.util.DAPResponseHandler;
import com.ebay.app.raptor.chocolate.adservice.util.ParametersParser;
import com.ebay.app.raptor.chocolate.adservice.constant.Errors;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.kernel.util.guid.Guid;
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
import java.net.URI;
import java.net.URISyntaxException;
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
  private static final String CLICK = "1";


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
  public boolean collectAr(HttpServletRequest request, HttpServletResponse response,
                           ContainerRequestContext requestContext) throws Exception {
    dapResponseHandler.sendDAPResponse(request, response, requestContext);
    return true;
  }

  /**
   * construct a tracking header. guid is mandatory, if guid is null, create a guid
   * @param requestContext request context
   * @param guid guid from mapping if there is
   * @param channelType channel type
   * @return a tracking header string
   */
  public String constructTrackingHeader(ContainerRequestContext requestContext, String guid,
                                        String channelType) {
    String cookie = "";
    String rawGuid = guid;
    if (!StringUtils.isEmpty(rawGuid) && rawGuid.length() >= Constants.GUID_LENGTH)
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

    return cookie;
  }

  /**
   * Collect email entry event(Adobe) and send redirect response
   *
   * @param request raw request
   * @return OK or Error message
   */
  public URI collectRedirect(HttpServletRequest request, ContainerRequestContext requestContext) throws Exception {

    // verify the request
    MultiValueMap<String, String> parameters = verifyAndParseRequest(request);

    // execute redirect Strategy
    return executeRedirectStrategy(request, getParam(parameters, Constants.PARTNER_ID), requestContext);
  }

  /**
   * Verify redirect request, if invalid then throw an exception
   */
  private MultiValueMap<String, String> verifyAndParseRequest(HttpServletRequest request)  throws Exception {
    // no query parameter, rejected
    Map<String, String[]> params = request.getParameterMap();
    if (null == params || params.isEmpty()) {
      logError(Errors.ERROR_REDIRECT_NO_QUERY_PARAMETER);
    }

    MultiValueMap<String, String> parameters = ParametersParser.parse(params);
    // reject no mkevt
    if (!parameters.containsKey(Constants.MKEVT)) {
      logError(Errors.REDIRECT_NO_MKEVT);
    }
    // reject invalid mkevt
    if (!CLICK.equals(getParam(parameters, Constants.MKEVT))) {
      logError(Errors.REDIRECT_INVALID_MKEVT);
    }
    // reject no mkcid
    if (!parameters.containsKey(Constants.MKCID)) {
      logError(Errors.REDIRECT_NO_MKCID);
    }
    // reject invalid mkcid
    if (ChannelIdEnum.parse(getParam(parameters, Constants.MKCID)) == null) {
      logError(Errors.REDIRECT_INVALID_MKCID);
    }

    return parameters;
  }

  /**
   * Send redirect response by Strategy Pattern
   */
  private URI executeRedirectStrategy(HttpServletRequest request, String partnerId, ContainerRequestContext context) throws URISyntaxException{
    RedirectContext redirectContext;
    if (Constants.ADOBE_PARTNER_ID.equals(partnerId)) {
      redirectContext = new RedirectContext(new AdobeRedirectStrategy());
    } else {
      redirectContext = new RedirectContext(new ThirdpartyRedirectStrategy());
    }

    try {
      return redirectContext.execute(request, context);
    } catch (Exception e) {
      metrics.meter("RedirectionRuntimeError");
      logger.warn("Redirection runtime error: ", e);
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

}
