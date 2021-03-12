/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */


package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.CommonConstant;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.component.GdprConsentHandler;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.util.*;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import java.net.URLDecoder;
import java.util.regex.Matcher;

import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_ACTION;
import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_TYPE;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ebaysites;
import static org.apache.commons.compress.utils.CharsetNames.UTF_8;

/**
 * @author xiangli4
 * Track
 * 1. ListenerMessage,
 * 2. Ubi message,
 * 3. Behavior message
 * for performance marketing channels
 */

@Component
@DependsOn("EventListenerService")
public class PerformanceMarketingCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceMarketingCollector.class);
  private Metrics metrics;
  private ListenerMessageParser parser;
  private BehaviorMessageParser behaviorMessageParser;
  private static final String CHECKOUT_API_USER_AGENT = "checkoutApi";

  @Autowired
  private GdprConsentHandler gdprConsentHandler;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
    this.parser = ListenerMessageParser.getInstance();
    this.behaviorMessageParser = BehaviorMessageParser.getInstance();
  }

  /**
   * @param requestContext      wrapped request context
   * @param targetUrl           landing page url
   * @param referer             referer
   * @param parameters          url parameters
   * @param channelType         channel type
   * @param channelAction       action type
   * @param request             http request
   * @param startTime           start timestamp of the request
   * @param endUserContext      enduserctx header
   * @param raptorSecureContext wrapped raptor secure context
   * @return                    Listener message
   */
  public ListenerMessage parseListenerMessage(ContainerRequestContext requestContext, String targetUrl, String referer,
                                              MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                              ChannelActionEnum channelAction, HttpServletRequest request,
                                              long startTime, IEndUserContext endUserContext,
                                              RaptorSecureContext raptorSecureContext) {
    // logic to filter internal redirection in node, https://jirap.corp.ebay.com/browse/XC-2361
    // currently we only observe the issue in vi pool in mweb case if the url does not contain title of the item
    // log metric here about the header which identifiers if there is a redirection
    String statusCodeStr = request.getHeader(Constants.NODE_REDIRECTION_HEADER_NAME);
    if (statusCodeStr != null) {
      int statusCode;

      try {
        statusCode = Integer.parseInt(statusCodeStr);
        if (statusCode == Response.Status.OK.getStatusCode()) {
          metrics.meter("CollectStatusOK", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
        } else if (statusCode >= Response.Status.MOVED_PERMANENTLY.getStatusCode() &&
            statusCode < Response.Status.BAD_REQUEST.getStatusCode()) {
          metrics.meter("CollectStatusRedirection", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
          LOGGER.debug("CollectStatusRedirection: URL: " + targetUrl + ", UA: " + endUserContext.getUserAgent());
        } else if (statusCode >= Response.Status.BAD_REQUEST.getStatusCode()) {
          metrics.meter("CollectStatusError", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
          LOGGER.error("CollectStatusError: " + targetUrl);
        } else {
          metrics.meter("CollectStatusDefault", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
        }
      } catch (NumberFormatException ex) {
        metrics.meter("StatusCodeError", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
            Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
        LOGGER.error("Error status code: " + statusCodeStr);
      }

    } else {
      metrics.meter("CollectStatusDefault", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
          Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
    }

    // parse rotation id
    long rotationId = parseRotationId(parameters);

    // parse campaign id
    long campaignId = -1L;
    try {
      campaignId = Long.parseLong(parameters.get(Constants.CAMPID).get(0));
    } catch (Exception e) {
      LOGGER.debug("No campaign id");
    }

    // get user id from auth token if it's user token, else we get from end user ctx
    String userId;
    if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
      userId = raptorSecureContext.getSubjectImmutableId();
    } else {
      userId = Long.toString(endUserContext.getOrigUserOracleId());
    }

    // parse session id for EPN channel
    String snid = "";
    if (channelType == ChannelIdEnum.EPN) {
      snid = parseSessionId(parameters);
    }

    // Parse the response
    ListenerMessage message = parser.parse(request, requestContext, startTime, campaignId, channelType
        .getLogicalChannel().getAvro(), channelAction, userId, endUserContext, targetUrl, referer, rotationId, snid);

    // Use the shot snapshot id from requests
    if (parameters.containsKey(Constants.MKRVRID) && parameters.get(Constants.MKRVRID).get(0) != null) {
      message.setShortSnapshotId(Long.valueOf(parameters.get(Constants.MKRVRID).get(0)));
    }

    // gdpr
    GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(targetUrl, channelType);
    boolean allowedStoredPersonalizedData = gdprConsentDomain.isAllowedStoredPersonalizedData();
    boolean allowedStoredContextualData = gdprConsentDomain.isAllowedStoredContextualData();
    if (message != null) {
      if (!allowedStoredContextualData) {
        message.setRemoteIp("");
        message.setUserAgent("");
        message.setGeoId(0L);
        message.setUdid("");
        message.setLangCd("");
        message.setReferer("");
        message.setRequestHeaders("");
      }
      if (!allowedStoredPersonalizedData) {
        message.setUserId(0L);
        message.setGuid(CommonConstant.EMPTY_GUID);
        message.setCguid(CommonConstant.EMPTY_GUID);
      }

      return message;
    }
    return message;
  }

  /**
   * @param requestContext  wrapped raptor request context
   * @param referer         referer of the request
   * @param parameters      url parameters
   * @param channelType     channel type
   * @param channelAction   action type
   * @param startTime       start time of the request
   * @param endUserContext  enduserctx header
   * @param message         listener message
   */
  public void trackUbi(ContainerRequestContext requestContext, String referer,
                       MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                       ChannelActionEnum channelAction, long startTime,
                       IEndUserContext endUserContext,
                       ListenerMessage message) {
    // Tracking ubi only when refer domain is not ebay. This should be moved to filter later.
    // Don't track ubi if it's AR
    // Don't track ubi if the click is from Checkout API
    // Don't track ubi if the click is a duplicate itm click
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if (!m.find() && !channelAction.equals(ChannelActionEnum.SERVE)
        && !isClickFromCheckoutAPI(channelType.getLogicalChannel().getAvro(), endUserContext)) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker =
            (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mkt", String.class);

        // rotation id
        requestTracker.addTag("rotid", String.valueOf(message.getDstRotationId()), String.class);

        // keyword
        String searchKeyword = "";
        if (parameters.containsKey(Constants.SEARCH_KEYWORD)
            && parameters.get(Constants.SEARCH_KEYWORD).get(0) != null) {

          searchKeyword = parameters.get(Constants.SEARCH_KEYWORD).get(0);
        }
        requestTracker.addTag("keyword", searchKeyword, String.class);

        // rvr id
        requestTracker.addTag("rvrid", message.getShortSnapshotId(), Long.class);

        // gclid
        String gclid = "";
        if (parameters.containsKey(Constants.GCLID) && parameters.get(Constants.GCLID).get(0) != null) {

          gclid = parameters.get(Constants.GCLID).get(0);
        }
        requestTracker.addTag("gclid", gclid, String.class);

        //producereventts
        requestTracker.addTag("producereventts", startTime, Long.class);

      } catch (Exception e) {
        LOGGER.warn("Error when tracking ubi for imk", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
            Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
          Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
    }
  }

  /**
   * @param requestContext  request context
   * @param targetUrl       target url
   * @param referer         referer of the request
   * @param parameters      parameters of url
   * @param channelType     channel type
   * @param channelAction   action type
   * @param request         http request
   * @param startTime       start time of the request
   * @param endUserContext  enduserctx header
   * @param agentInfo       user agent
   * @param message         listener message
   * @return                behavior message
   */
  public BehaviorMessage parseBehaviorMessage(ContainerRequestContext requestContext, String targetUrl, String referer,
                                              MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                              ChannelActionEnum channelAction, HttpServletRequest request,
                                              long startTime, IEndUserContext endUserContext, UserAgentInfo agentInfo,
                                              ListenerMessage message) {
    BehaviorMessage behaviorMessage = null;
    switch (channelAction) {
      case CLICK:
        behaviorMessage = behaviorMessageParser.parseAmsAndImkEvent(request, requestContext, endUserContext,
            parameters, agentInfo, targetUrl, startTime, channelType.getLogicalChannel().getAvro(),
            channelAction.getAvro(), message.getShortSnapshotId(), PageIdEnum.CLICK.getId(),
            PageNameEnum.CLICK.getName(), 0, referer, message.getGuid(), message.getCguid(),
            String.valueOf(message.getUserId()), String.valueOf(message.getDstRotationId()));
        break;
      case SERVE:
        behaviorMessage = behaviorMessageParser.parseAmsAndImkEvent(request, requestContext, endUserContext,
            parameters, agentInfo, targetUrl, startTime, channelType.getLogicalChannel().getAvro(),
            channelAction.getAvro(), message.getShortSnapshotId(), PageIdEnum.AR.getId(),
            PageNameEnum.ADREQUEST.getName(), 0, referer, message.getGuid(), message.getCguid(),
            String.valueOf(message.getUserId()), String.valueOf(message.getDstRotationId()));
        break;
      default:
        break;
    }
    return behaviorMessage;
  }

  /**
   * Parse rotation id from query mkrid
   */
  private long parseRotationId(MultiValueMap<String, String> parameters) {
    long rotationId = -1L;
    if (parameters.containsKey(Constants.MKRID) && parameters.get(Constants.MKRID).get(0) != null) {
      try {
        String rawRotationId = parameters.get(Constants.MKRID).get(0);
        // decode rotationId if rotation is encoded
        // add decodeCnt to avoid looping infinitely
        int decodeCnt = 0;
        while (rawRotationId.contains("%") && decodeCnt < 5) {
          rawRotationId = URLDecoder.decode(rawRotationId, UTF_8);
          decodeCnt = decodeCnt + 1;
        }
        rotationId = Long.parseLong(rawRotationId.replaceAll("-", ""));
      } catch (Exception e) {
        LOGGER.warn(Errors.ERROR_INVALID_MKRID);
        metrics.meter("InvalidMkrid");
      }
    } else {
      LOGGER.warn(Errors.ERROR_NO_MKRID);
      metrics.meter("NoMkrid");
    }

    return rotationId;
  }

  /**
   * Parse session id from query mksid for epn channel
   */
  private String parseSessionId(MultiValueMap<String, String> parameters) {
    String sessionId = "";
    if (parameters.containsKey(Constants.MKSID) && parameters.get(Constants.MKSID).get(0) != null) {
      try {
        sessionId = parameters.get(Constants.MKSID).get(0);
      } catch (Exception e) {
        LOGGER.warn(Errors.ERROR_INVALID_MKSID);
        metrics.meter("InvalidMksid");
      }
    } else {
      LOGGER.warn(Errors.ERROR_NO_MKSID);
      metrics.meter("NoMksid");
    }

    return sessionId;
  }

  /**
   * Determine whether the click is from Checkout API
   * If so, don't track into ubi
   */
  private Boolean isClickFromCheckoutAPI(ChannelType channelType, IEndUserContext endUserContext) {
    boolean isClickFromCheckoutAPI = false;
    try {
      if (channelType == ChannelType.EPN && endUserContext.getUserAgent().equals(CHECKOUT_API_USER_AGENT)) {
        isClickFromCheckoutAPI = true;
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click from Checkout API error");
      metrics.meter("DetermineCheckoutAPIClickError", 1);
    }
    return isClickFromCheckoutAPI;
  }
}
