/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */


package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.CommonConstant;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.component.GdprConsentHandler;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.eventlistener.util.ListenerMessageParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.SearchEngineFreeListingsRotationEnum;
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.ws.rs.container.ContainerRequestContext;

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
  ListenerMessageParser parser;
  private static final String CHECKOUT_API_USER_AGENT = "checkoutApi";

  @Autowired
  private GdprConsentHandler gdprConsentHandler;

  @PostConstruct
  public void postInit() {
    this.parser = ListenerMessageParser.getInstance();
  }

  /**
   * Parse listener message and handle GDPR
   * @param baseEvent base event
   * @return ListenerMessage
   */
  public ListenerMessage decorateListenerMessageAndHandleGDPR(BaseEvent baseEvent) {

    ListenerMessage message = parser.parse(baseEvent);

    // Use the shot snapshot id from requests
    if (baseEvent.getUrlParameters().containsKey(Constants.MKRVRID)
        && baseEvent.getUrlParameters().get(Constants.MKRVRID).get(0) != null) {
      message.setShortSnapshotId(Long.valueOf(baseEvent.getUrlParameters().get(Constants.MKRVRID).get(0)));
    }

    // gdpr
    GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(baseEvent.getUrl(),
        baseEvent.getChannelType());
    eraseByGdpr(gdprConsentDomain, message);
    return message;
  }

  void eraseByGdpr(GdprConsentDomain gdprConsentDomain, ListenerMessage message) {
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
    }
  }

  public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent, ListenerMessage message) {
    IRequestScopeTracker requestTracker
            = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

    // page id
    requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.CLICK.getId(), Integer.class);

    // event action
    requestTracker.addTag(TrackerTagValueUtil.EventActionTag, Constants.EVENT_ACTION, String.class);

    // target url
    if (StringUtils.isNotBlank(baseEvent.getUrl())) {
      requestTracker.addTag(SOJ_MPRE_TAG, baseEvent.getUrl(), String.class);
    }

    // referer
    if (StringUtils.isNotBlank(baseEvent.getReferer())) {
      requestTracker.addTag("ref", baseEvent.getReferer(), String.class);
    }

    // utp event id
    if (StringUtils.isNotBlank(baseEvent.getUuid())) {
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
    if (baseEvent.getUrlParameters().containsKey(Constants.GCLID)
            && baseEvent.getUrlParameters().get(Constants.GCLID).get(0) != null) {
      gclid = baseEvent.getUrlParameters().get(Constants.GCLID).get(0);
    }
    requestTracker.addTag("gclid", gclid, String.class);

    // producereventts
    requestTracker.addTag("producereventts", baseEvent.getTimestamp(), Long.class);

    // UFES Tag
    String ufesEdgTrkSvcHeader = baseEvent.getRequestHeaders().get(UFES_EDGTRKSVC_HDR);
    if (StringUtils.isNotBlank(ufesEdgTrkSvcHeader)) {
      requestTracker.addTag(UFES_EDGTRKSVC_HDR, ufesEdgTrkSvcHeader, String.class);
    }

    if (baseEvent.getChannelType().equals(ChannelIdEnum.SOCIAL_MEDIA)) {
      socialMediaParamTags.forEach((key, val) ->
              addTagFromUrlQuery(baseEvent.getUrlParameters(), requestTracker, val, key, String.class));
    }

    // channel id
    requestTracker.addTag(TAG_CHANNEL, baseEvent.getChannelType().getValue(), String.class);
  }

  public String getSearchEngineFreeListingsRotationId(UserPrefsCtx userPrefsCtx) {
    int siteId = userPrefsCtx.getGeoContext().getSiteId();
    return SearchEngineFreeListingsRotationEnum.parse(siteId).getRotation();
  }

  /**
   * Set timestamp for checkout api click
   * @param baseEvent base event
   */
  public BaseEvent setCheckoutTimestamp(BaseEvent baseEvent) {
    // update startTime if the click comes from checkoutAPI
    if (baseEvent.getChannelType() == ChannelIdEnum.EPN) {
      EventPayload payload = baseEvent.getPayload();
      if (payload != null) {
        String checkoutAPIClickTs = payload.getCheckoutAPIClickTs();
        if (!StringUtils.isEmpty(checkoutAPIClickTs)) {
          try {
            long checkoutAPIClickTimestamp = Long.parseLong(checkoutAPIClickTs);
            if (checkoutAPIClickTimestamp > 0) {
              baseEvent.setTimestamp(checkoutAPIClickTimestamp);
            }
          } catch (Exception e) {
            LOGGER.warn(e.getMessage());
            LOGGER.warn("Error click timestamp from Checkout API " + checkoutAPIClickTs);
            MonitorUtil.info("ErrorCheckoutAPIClickTimestamp", 1);
          }
        }
      }
    }
    return baseEvent;
  }

  private void addTagFromUrlQuery(MultiValueMap<String, String> parameters, IRequestScopeTracker requestTracker,
                                  String urlParam, String tag, Class tagType) {
    if (parameters.containsKey(urlParam) && parameters.get(urlParam).get(0) != null) {
      requestTracker.addTag(tag, parameters.get(urlParam).get(0), tagType);
    }
  }
}
