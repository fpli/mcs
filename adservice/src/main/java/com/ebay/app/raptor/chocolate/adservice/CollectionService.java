package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.EmailPartnerIdEnum;
import com.ebay.app.raptor.chocolate.adservice.constant.Errors;
import com.ebay.app.raptor.chocolate.adservice.redirect.AdobeRedirectStrategy;
import com.ebay.app.raptor.chocolate.adservice.redirect.RedirectContext;
import com.ebay.app.raptor.chocolate.adservice.redirect.ThirdpartyRedirectStrategy;
import com.ebay.app.raptor.chocolate.adservice.util.*;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
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
  private static final String CLICK = "1";

  @Autowired
  private DAPResponseHandler dapResponseHandler;

  @Autowired
  private AdserviceCookie adserviceCookie;

  @Autowired
  private EpntResponseHandler epntResponseHandler;

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
                           GdprConsentDomain gdprConsentDomain) throws Exception {
    dapResponseHandler.sendDAPResponse(request, response, gdprConsentDomain);
    return true;
  }

  /**
   * Collect email entry event(Adobe) and send redirect response
   *
   * @param request raw request
   * @return OK or Error message
   */
  public URI collectRedirect(HttpServletRequest request, HttpServletResponse response, Client mktClient,
                             String endpoint)
      throws Exception {

    // verify the request
    MultiValueMap<String, String> parameters = verifyAndParseRequest(request);

    // execute redirect Strategy
    return executeRedirectStrategy(request, response, getParam(parameters, Constants.MKPID), mktClient, endpoint);
  }

  /**
   * Verify redirect request, if invalid then throw an exception
   */
  private MultiValueMap<String, String> verifyAndParseRequest(HttpServletRequest request)  throws Exception {
    // no query parameter, redirect to home page
    Map<String, String[]> params = request.getParameterMap();
    if (null == params || params.isEmpty()) {
      logError(Errors.REDIRECT_NO_QUERY_PARAMETER);
    }

    MultiValueMap<String, String> parameters = HttpUtil.parse(params);
    // no mkevt, redirect to home page
    if (!parameters.containsKey(Constants.MKEVT) || parameters.getFirst(Constants.MKEVT) == null) {
      logError(Errors.REDIRECT_NO_MKEVT);
    }
    // invalid mkevt, redirect to home page
    if (!CLICK.equals(getParam(parameters, Constants.MKEVT))) {
      logError(Errors.REDIRECT_INVALID_MKEVT);
    }
    // no mkcid, redirect to home page
    if (!parameters.containsKey(Constants.MKCID) || parameters.getFirst(Constants.MKCID) == null) {
      logError(Errors.REDIRECT_NO_MKCID);
    }
    // invalid mkcid, redirect to home page
    if (ChannelIdEnum.parse(getParam(parameters, Constants.MKCID)) == null) {
      logError(Errors.REDIRECT_INVALID_MKCID);
    }
    // no mkpid, redirect to home page
    if (!parameters.containsKey(Constants.MKPID) || parameters.getFirst(Constants.MKPID) == null) {
      logError(Errors.REDIRECT_NO_MKPID);
    }
    // invalid mkpid, accepted
    if (EmailPartnerIdEnum.parse(getParam(parameters, Constants.MKPID)) == null) {
      logger.warn(Errors.REDIRECT_INVALID_MKPID);
      MonitorUtil.info(Errors.REDIRECT_INVALID_MKPID);
    }

    return parameters;
  }

  /**
   * Send redirect response by Strategy Pattern
   */
  private URI executeRedirectStrategy(HttpServletRequest request, HttpServletResponse response, String partnerId,
                                      Client mktClient, String endpoint) throws URISyntaxException {
    RedirectContext redirectContext;
    if (EmailPartnerIdEnum.ADOBE.getId().equals(partnerId)) {
      redirectContext = new RedirectContext(new AdobeRedirectStrategy());
    } else {
      redirectContext = new RedirectContext(new ThirdpartyRedirectStrategy());
    }

    String guid = adserviceCookie.getGuid(request);
    String adguid = adserviceCookie.readAdguid(request, response);

    try {
      return redirectContext.execute(request, mktClient, endpoint, guid, adguid);
    } catch (Exception e) {
      MonitorUtil.info("RedirectionRuntimeError");
      logger.warn("Redirection runtime error: ", e);
    }

    return new URIBuilder(ApplicationOptions.getInstance().getRedirectHomepage()).build();
  }

  /**
   * log error, log metric and throw error with error key
   *
   * @param  error error type
   * @throws Exception exception with error key
   */
  private void logError(String error) throws Exception {
    logger.warn(error);
    MonitorUtil.info(error);
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

  /**
   * Call epnt placement api and return response
   *
   * @param request
   * @return response
   */
  public Response collectEpntPlacementRedirect(HttpServletRequest request, HttpServletResponse response) throws Exception {
    return epntResponseHandler.callEpntPlacementResponse(request, response);
  }

}
