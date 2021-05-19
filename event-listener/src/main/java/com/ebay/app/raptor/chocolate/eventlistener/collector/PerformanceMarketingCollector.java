/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */


package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.CommonConstant;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.component.GdprConsentHandler;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.util.*;
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.geo.context.UserPrefsCtx;
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
import org.springframework.util.StringUtils;
import javax.annotation.PostConstruct;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import static com.ebay.app.raptor.chocolate.constant.Constants.*;

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
  Metrics metrics;
  ListenerMessageParser parser;
  private static final String CHECKOUT_API_USER_AGENT = "checkoutApi";

  @Autowired
  private GdprConsentHandler gdprConsentHandler;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
    this.parser = ListenerMessageParser.getInstance();
  }

  /**
   * Parse listener message and handle GDPR
   * @param baseEvent base event
   * @return ListenerMessage
   */
  public ListenerMessage decorateListenerMessageAndHandleGDPR(BaseEvent baseEvent) {

    ChannelIdEnum channelIdEnum = baseEvent.getChannelType();
    ChannelActionEnum channelActionEnum = baseEvent.getActionType();
    // logic to filter internal redirection in node, https://jirap.corp.ebay.com/browse/XC-2361
    // currently we only observe the issue in vi pool in mweb case if the url does not contain title of the item
    // log metric here about the header which identifiers if there is a redirection
    String statusCodeStr = baseEvent.getRequestHeaders().get(Constants.NODE_REDIRECTION_HEADER_NAME);
    if (statusCodeStr != null) {
      int statusCode;

      try {
        statusCode = Integer.parseInt(statusCodeStr);
        if (statusCode == Response.Status.OK.getStatusCode()) {
          metrics.meter("CollectStatusOK", 1, Field.of(CHANNEL_ACTION, channelActionEnum.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelIdEnum.getLogicalChannel().getAvro().toString()));
        } else if (statusCode >= Response.Status.MOVED_PERMANENTLY.getStatusCode() &&
            statusCode < Response.Status.BAD_REQUEST.getStatusCode()) {
          metrics.meter("CollectStatusRedirection", 1, Field.of(CHANNEL_ACTION, channelActionEnum.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelIdEnum.getLogicalChannel().getAvro().toString()));
          LOGGER.debug("CollectStatusRedirection: URL: " + baseEvent.getUrl() + ", UA: " +
              baseEvent.getEndUserContext().getUserAgent());
        } else if (statusCode >= Response.Status.BAD_REQUEST.getStatusCode()) {
          metrics.meter("CollectStatusError", 1, Field.of(CHANNEL_ACTION, channelActionEnum.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelIdEnum.getLogicalChannel().getAvro().toString()));
          LOGGER.error("CollectStatusError: " + baseEvent.getUrl());
        } else {
          metrics.meter("CollectStatusDefault", 1, Field.of(CHANNEL_ACTION, channelActionEnum.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelIdEnum.getLogicalChannel().getAvro().toString()));
        }
      } catch (NumberFormatException ex) {
        metrics.meter("StatusCodeError", 1, Field.of(CHANNEL_ACTION, channelActionEnum.getAvro().toString()),
            Field.of(CHANNEL_TYPE, channelIdEnum.getLogicalChannel().getAvro().toString()));
        LOGGER.error("Error status code: " + statusCodeStr);
      }

    } else {
      metrics.meter("CollectStatusDefault", 1, Field.of(CHANNEL_ACTION, channelActionEnum.getAvro().toString()),
          Field.of(CHANNEL_TYPE, channelIdEnum.getLogicalChannel().getAvro().toString()));
    }

    ListenerMessage message = parser.parse(baseEvent);

    // Use the shot snapshot id from requests
    if (baseEvent.getUrlParameters().containsKey(Constants.MKRVRID)
        && baseEvent.getUrlParameters().get(Constants.MKRVRID).get(0) != null) {
      message.setShortSnapshotId(Long.valueOf(baseEvent.getUrlParameters().get(Constants.MKRVRID).get(0)));
    }

    // gdpr
    GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(baseEvent.getUrl(),
        baseEvent.getChannelType());
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

  public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent, ListenerMessage message) {
    // Tracking ubi only when refer domain is not ebay. This should be moved to filter later.
    // Don't track ubi if the click is from Checkout API
    if (isClickFromCheckoutAPI(baseEvent.getChannelType().getLogicalChannel().getAvro(),
        baseEvent.getEndUserContext())) {
      metrics.meter("CheckoutAPIClick", 1);
    } else {
      try {

        IRequestScopeTracker requestTracker
            = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // page id
        if (baseEvent.getActionType().equals(ChannelActionEnum.CLICK)) {
          requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.CLICK.getId(), Integer.class);
        } else if (baseEvent.getActionType().equals(ChannelActionEnum.ROI)) {
          requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.ROI.getId(), Integer.class);
        }

        // event action
        requestTracker.addTag(TrackerTagValueUtil.EventActionTag, Constants.EVENT_ACTION, String.class);

        // target url
        if (!StringUtils.isEmpty(baseEvent.getUrl())) {
          requestTracker.addTag(SOJ_MPRE_TAG, baseEvent.getUrl(), String.class);
        }

        // referer
        if (!StringUtils.isEmpty(baseEvent.getReferer())) {
          requestTracker.addTag("ref", baseEvent.getReferer(), String.class);
        }

        // utp event id
        if (!StringUtils.isEmpty(baseEvent.getUuid())) {
          requestTracker.addTag("utpid", baseEvent.getUuid(), String.class);
        }

        // populate device info
        CollectionServiceUtil.populateDeviceDetectionParams(
            (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME), requestTracker);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mkt", String.class);

        // rotation id
        requestTracker.addTag("rotid", String.valueOf(message.getDstRotationId()), String.class);

        // keyword
        String searchKeyword = "";
        if (baseEvent.getUrlParameters().containsKey(Constants.SEARCH_KEYWORD)
            && baseEvent.getUrlParameters().get(Constants.SEARCH_KEYWORD).get(0) != null) {

          searchKeyword = baseEvent.getUrlParameters().get(Constants.SEARCH_KEYWORD).get(0);
        }
        requestTracker.addTag("keyword", searchKeyword, String.class);

        // rvr id
        requestTracker.addTag("rvrid", message.getShortSnapshotId(), Long.class);

        // gclid
        String gclid = "";
        if (baseEvent.getUrlParameters().containsKey(Constants.GCLID) &&
            baseEvent.getUrlParameters().get(Constants.GCLID).get(0) != null) {

          gclid = baseEvent.getUrlParameters().get(Constants.GCLID).get(0);
        }
        requestTracker.addTag("gclid", gclid, String.class);

        //producereventts
        requestTracker.addTag("producereventts", baseEvent.getTimestamp(), Long.class);

      } catch (Exception e) {
        LOGGER.warn("Error when tracking ubi for imk", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, baseEvent.getActionType().getAvro().toString()),
            Field.of(CHANNEL_TYPE, baseEvent.getChannelType().getLogicalChannel().getAvro().toString()));
      }
    }
  }

  /**
   * Determine whether the click is from Checkout API
   * If so, don't track into ubi
   */
  Boolean isClickFromCheckoutAPI(ChannelType channelType, IEndUserContext endUserContext) {
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

  public String getSearchEngineFreeListingsRotationId(UserPrefsCtx userPrefsCtx) {
    int siteId = userPrefsCtx.getGeoContext().getSiteId();
    return SearchEngineFreeListingsRotationEnum.parse(siteId).getRotation();
  }

  /**
   * Set flag from checkout api
   * @param baseEvent base event
   */
  public BaseEvent setCheckoutApiFlag(BaseEvent baseEvent) {
    // update startTime if the click comes from checkoutAPI
    if (baseEvent.getChannelType() == ChannelIdEnum.EPN) {
      EventPayload payload = baseEvent.getPayload();
      if (payload != null) {
        String checkoutAPIClickTs = payload.getCheckoutAPIClickTs();
        if (!StringUtils.isEmpty(checkoutAPIClickTs)) {
          try {
            long checkoutAPIClickTimestamp = Long.parseLong(checkoutAPIClickTs);
            if (checkoutAPIClickTimestamp > 0) {
              baseEvent.setCheckoutApi(true);
              baseEvent.setTimestamp(checkoutAPIClickTimestamp);
            }
          } catch (Exception e) {
            LOGGER.warn(e.getMessage());
            LOGGER.warn("Error click timestamp from Checkout API " + checkoutAPIClickTs);
            metrics.meter("ErrorCheckoutAPIClickTimestamp", 1);
          }
        }
      }
    }
    return baseEvent;
  }
}
