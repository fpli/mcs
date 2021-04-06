/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.util.BehaviorMessageParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageIdEnum;
import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.StringTokenizer;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ebaysites;

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
   *
   * @param parameters    url parameters
   * @param type          channel type
   * @param action        action type
   * @param request       http request
   * @param uri           url
   * @param channelAction channel action enum
   */
  public void trackUbi(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters, String type,
                       String action, HttpServletRequest request, String uri, String referer, String utpEventId,
                       ChannelAction channelAction) {
    // add common tags
    addCommonTags(requestContext, uri, referer,
        (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME), utpEventId, type, action,
        PageIdEnum.CLICK.getId());

    // add tags in url param "sojTags"
    // Don't track ubi if the click is a duplicate itm click
    if (parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      addGenericSojTags(requestContext, parameters, type, action);
    }
  }

  /**
   * @param requestContext request context
   * @param endUserContext enduserctx header
   * @param referer        referer of the request
   * @param parameters     url parameters
   * @param request        http request
   * @param agentInfo      user agent
   * @param uri            target url
   * @param startTime      start time of the request
   * @param channelType    channel type
   * @param channelAction  action type
   * @param snapshotId     snapshot id
   * @return behavior message
   */
  public BehaviorMessage parseBehaviorMessage(ContainerRequestContext requestContext, IEndUserContext endUserContext,
                                              String referer, MultiValueMap<String, String> parameters,
                                              HttpServletRequest request, UserAgentInfo agentInfo, String uri,
                                              Long startTime, ChannelType channelType, ChannelAction channelAction,
                                              long snapshotId) {
    return behaviorMessageParser.parse(request, requestContext, endUserContext, parameters,
        agentInfo, referer, uri, startTime, channelType, channelAction, snapshotId, 0);
  }

  /**
   * Add common soj tags all channels in common
   *
   * @param requestContext wrapped raptor request context
   * @param targetUrl      landing page url
   * @param referer        referer of the request
   * @param agentInfo      user agent
   * @param utpEventId     utp event id
   * @param type           channel type
   * @param action         action type
   * @param pageId         soj page id
   */
  private void addCommonTags(ContainerRequestContext requestContext, String targetUrl, String referer,
                             UserAgentInfo agentInfo, String utpEventId, String type, String action, int pageId) {
    try {
      // Ubi tracking
      IRequestScopeTracker requestTracker
          = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      // page id
      requestTracker.addTag(TrackerTagValueUtil.PageIdTag, pageId, Integer.class);

      // event action
      requestTracker.addTag(TrackerTagValueUtil.EventActionTag, Constants.EVENT_ACTION, String.class);

      // target url
      if (!StringUtils.isEmpty(targetUrl)) {
        requestTracker.addTag(SOJ_MPRE_TAG, targetUrl, String.class);
      }

      // referer
      if (!StringUtils.isEmpty(referer)) {
        requestTracker.addTag("ref", referer, String.class);
      }

      // utp event id
      if (!StringUtils.isEmpty(utpEventId)) {
        requestTracker.addTag("utpid", utpEventId, String.class);
      }

      // populate device info
      CollectionServiceUtil.populateDeviceDetectionParams(agentInfo, requestTracker);

    } catch (Exception e) {
      LOGGER.warn("Error when tracking ubi for common tags", e);
      metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }
  }

  /**
   * Add generic soj tags for email channel. Those tags are defined in URL which needed to be added as a tag.
   *
   * @param requestContext wrapped raptor request context
   * @param parameters     url parameters
   * @param type           channel type
   * @param action         action type
   */
  void addGenericSojTags(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                         String type, String action) {

    // Ubi tracking
    IRequestScopeTracker requestTracker
        = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

    String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
    try {
      sojTags = URLDecoder.decode(sojTags, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOGGER.warn("Param sojTags is wrongly encoded", e);
      metrics.meter("ErrorEncodedSojTags", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
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
          LOGGER.warn("Error when tracking ubi for common tags", e);
          metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
        }
      }
    }
  }

  /**
   * Soj tag fbprefetch
   */
  static boolean isFacebookPrefetchEnabled(HttpServletRequest request) {
    String facebookprefetch = request.getHeader("X-Purpose");
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
}
