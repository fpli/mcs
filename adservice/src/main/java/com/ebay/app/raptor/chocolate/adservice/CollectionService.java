package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.adservice.redirect.AdobeRedirectStrategy;
import com.ebay.app.raptor.chocolate.adservice.redirect.RedirectContext;
import com.ebay.app.raptor.chocolate.adservice.util.*;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.Errors;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  // do not filter /ulk XC-1541
  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);

  @Autowired
  private HttpClientConnectionManager httpClientConnectionManager;

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
  public boolean collectImpression(HttpServletRequest request, HttpServletResponse response,
                                   ContainerRequestContext requestContext) throws Exception {
    // send 1x1 pixel
    ImageResponseHandler.sendImageResponse(response);
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
  public boolean collectRdirect(HttpServletRequest request, HttpServletResponse response,
                                ContainerRequestContext requestContext) throws Exception {

    // verify the request
    verifyRedirectRequest(request);

    // get referer from request
    String referer = request.getHeader("Referer");
    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase("null")) {
      logger.warn(Errors.ERROR_NO_REFERER);
      metrics.meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }
    // decode referer if necessary. Currently, android is sending rover url encoded.
    if(referer.startsWith("https%3A%2F%2") || referer.startsWith("http%3A%2F%2")) {
      referer = URLDecoder.decode( referer, "UTF-8" );
    }

    // get parameters from request
    MultiValueMap<String, String> parameters = ParametersParser.parse(request.getParameterMap());
    ChannelIdEnum channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    String type = channelType.getLogicalChannel().getAvro().toString();
    String partnerId = parameters.get(Constants.PARTNER_ID).get(0);

    // execute redirect Strategy
    executeRedirectStrategy(request, response, partnerId, requestContext);

    // store data to Soj
    addGenericSojTags(requestContext, parameters, referer, type, ChannelActionEnum.CLICK.name());

    // TODO: store data to Chocolate
    return true;
  }

  /**
   * Verify redirect request, if invalid then throw an exception
   */
  private void verifyRedirectRequest(HttpServletRequest request)  throws Exception {
    // no user agent, rejected
    String userAgent = request.getHeader("User-Agent");
    if (null == userAgent) {
      logError(Errors.ERROR_NO_USER_AGENT);
    }

    // no query parameter, rejected
    Map<String, String[]> params = request.getParameterMap();
    if (params.isEmpty()) {
      logError(Errors.ERROR_NO_QUERY_PARAMETER);
    }
    MultiValueMap<String, String> parameters = ParametersParser.parse(params);

    // no partnerId, rejected
    if (!parameters.containsKey(Constants.PARTNER_ID) || parameters.get(Constants.PARTNER_ID).get(0) == null) {
      logError(Errors.ERROR_NO_PARTNER_ID);
    }

    // no mkcid, rejected
    if (!parameters.containsKey(Constants.MKCID) || parameters.get(Constants.MKCID).get(0) == null) {
      logError(Errors.ERROR_NO_MKCID);
    }

    // invalid mkcid, rejected
    ChannelIdEnum channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      logError(Errors.ERROR_INVALID_MKCID);
    }
  }

  /**
   * Send redirect response by Strategy Pattern
   */
  private void executeRedirectStrategy(HttpServletRequest request, HttpServletResponse response, String partnerId, ContainerRequestContext requestContext) throws Exception {
    RedirectContext redirectContext = null;
    switch (partnerId) {
      case ADOBE_PARTNER_ID:
        redirectContext = new RedirectContext(new AdobeRedirectStrategy(httpClientConnectionManager, requestContext));
        break;
      default:
        logError(Errors.ERROR_INVALID_PARTNER_ID);
    }
    if (redirectContext != null) {
      try {
        redirectContext.execute(request, response);
      } catch (IOException e) {
        logError(Errors.ERROR_REDIRECT_RUNTIME);
      }
    } else {
      logError(Errors.ERROR_INVALID_PARTNER_ID);
    }
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

  private void addGenericSojTags(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                                 String referer, String type, String action) {
    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      // Ubi tracking
      IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      // Format the sojTags field when it contains UTF-8 character
      if(sojTags.contains("%3D")) {
        try {
          sojTags = URLDecoder.decode( sojTags, "UTF-8" );
        } catch (UnsupportedEncodingException e) {
          logger.warn("sojTags is wrong format", e);
        }
      }
      if (!StringUtils.isEmpty(sojTags)) {
        StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
        while (stToken.hasMoreTokens()) {
          try {
            StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
            if (sojNvp.countTokens() == 2) {
              String sojTag = sojNvp.nextToken().trim();
              String urlParam = sojNvp.nextToken().trim();
              if (!StringUtils.isEmpty(urlParam) && !StringUtils.isEmpty(sojTag)) {
                addTagFromUrlQuery(parameters, requestTracker, urlParam, sojTag, String.class);
              }
            }
          } catch (Exception e) {
            logger.warn("Error when tracking ubi for common tags", e);
            metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
          }
        }
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }
  }

  /**
   * Parse tag from url query string and add to sojourner
   */
  private static void addTagFromUrlQuery(MultiValueMap<String, String> parameters, IRequestScopeTracker requestTracker,
                                         String urlParam, String tag, Class tagType) {
    if (parameters.containsKey(urlParam) && parameters.get(urlParam).get(0) != null) {
      requestTracker.addTag(tag, parameters.get(urlParam).get(0), tagType);
    }
  }
}