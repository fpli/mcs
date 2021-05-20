/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.util.BehaviorMessageParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.eventlistener.util.EmailPartnerIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageIdEnum;
import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import com.google.common.primitives.Longs;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.StringTokenizer;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;

/**
 * @author xiangli4
 * Track customer marketing channels
 * 1. Ubi message,
 * 2. Behavior message
 */
public abstract class CustomerMarketingCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomerMarketingCollector.class);

  private BehaviorMessageParser behaviorMessageParser;
  Metrics metrics;

  public void postInit() {
    this.metrics = ESMetrics.getInstance();
    this.behaviorMessageParser = BehaviorMessageParser.getInstance();
  }

  /**
   * Super class handles common tags and sojtags in CM urls
   * @param requestContext request context
   * @param baseEvent base event
   */
  public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent) {
    // add common tags
    addCommonTags(requestContext, baseEvent, PageIdEnum.CLICK.getId());

    // add tags in url param "sojTags"
    // Don't track ubi if the click is a duplicate itm click
    if (baseEvent.getUrlParameters().containsKey(Constants.SOJ_TAGS)
        && baseEvent.getUrlParameters().get(Constants.SOJ_TAGS).get(0) != null) {
      addGenericSojTags(requestContext, baseEvent);
    }
  }

  /**
   * Add common soj tags all channels in common
   *
   * @param requestContext wrapped raptor request context
   * @param baseEvent      base event
   * @param pageId         soj page id
   */
  private void addCommonTags(ContainerRequestContext requestContext, BaseEvent baseEvent, int pageId) {
    // Ubi tracking
    IRequestScopeTracker requestTracker
        = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

    // page id
    requestTracker.addTag(TrackerTagValueUtil.PageIdTag, pageId, Integer.class);

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
    CollectionServiceUtil.populateDeviceDetectionParams(baseEvent.getUserAgentInfo(), requestTracker);
  }

  /**
   * Add generic soj tags for email channel. Those tags are defined in URL which needed to be added as a tag.
   *
   * @param requestContext wrapped raptor request context
   * @param baseEvent base event
   */
  void addGenericSojTags(ContainerRequestContext requestContext, BaseEvent baseEvent) {

    // Ubi tracking
    IRequestScopeTracker requestTracker
        = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

    String sojTags = baseEvent.getUrlParameters().get(Constants.SOJ_TAGS).get(0);
    try {
      sojTags = URLDecoder.decode(sojTags, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOGGER.warn("Param sojTags is wrongly encoded", e);
      metrics.meter("ErrorEncodedSojTags", 1, Field.of(CHANNEL_ACTION, baseEvent.getActionType()),
          Field.of(CHANNEL_TYPE, baseEvent.getChannelType()));
    }
    if (!StringUtils.isEmpty(sojTags)) {
      StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
      while (stToken.hasMoreTokens()) {
        StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
        if (sojNvp.countTokens() == 2) {
          String sojTag = sojNvp.nextToken().trim();
          String urlParam = sojNvp.nextToken().trim();
          if (!StringUtils.isEmpty(urlParam) && !StringUtils.isEmpty(sojTag)) {
            addTagFromUrlQuery(baseEvent.getUrlParameters(), requestTracker, urlParam, sojTag, String.class);
          }
        }
      }
    }
  }

  /**
   * Soj tag fbprefetch
   */
  static boolean isFacebookPrefetchEnabled(Map<String, String> requestHeaders) {
    String facebookprefetch = requestHeaders.get("X-Purpose");
    return facebookprefetch != null && "preview".equals(facebookprefetch.trim());
  }

  /**
   * Parse tag from url query string and add to sojourner
   */
  static void addTagFromUrlQuery(MultiValueMap<String, String> parameters, IRequestScopeTracker requestTracker,
                                 String urlParam, String tag, Class tagType) {
    if (parameters.containsKey(urlParam) && parameters.get(urlParam).get(0) != null) {
      requestTracker.addTag(tag, parameters.get(urlParam).get(0), tagType);
    }
  }

  static void addDecrytpedUserIDFromBu(MultiValueMap<String, String> parameters,
                                       IRequestScopeTracker requestTracker) {
    if (parameters.containsKey(Constants.BEST_GUESS_USER)) {
      String bu = parameters.get(Constants.BEST_GUESS_USER).get(0);
      Long encryptedUserId = Longs.tryParse(bu);
      if (encryptedUserId != null) {
        requestTracker.addTag("u", String.valueOf(EncryptUtil.decryptUserId(encryptedUserId)), String.class);
      }
    }
  }

  @Nullable
  public String getEmailPartner(MultiValueMap<String, String> parameters, ChannelIdEnum channelType) {
    // check partner for email click
    String partner = null;
    if (ChannelIdEnum.SITE_EMAIL.equals(channelType) || ChannelIdEnum.MRKT_EMAIL.equals(channelType)) {
      // no mkpid, accepted
      if (!parameters.containsKey(Constants.MKPID) || parameters.get(Constants.MKPID).get(0) == null) {
        LOGGER.warn(Errors.ERROR_NO_MKPID);
        metrics.meter("NoMkpidParameter");
      } else {
        // invalid mkpid, accepted
        partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
        if (StringUtils.isEmpty(partner)) {
          LOGGER.warn(Errors.ERROR_INVALID_MKPID);
          metrics.meter("InvalidMkpid");
        }
      }
    }
    return partner;
  }
}
