/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.util.BehaviorMessageParser;
import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.google.common.primitives.Longs;
import org.springframework.util.MultiValueMap;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * @author xiangli4
 * Track customer marketing channels
 * 1. Ubi message,
 * 2. Behavior message
 */
public abstract class CustomerMarketingCollector {
  BehaviorMessageParser behaviorMessageParser;
  Metrics metrics;

  public void postInit() {
    this.metrics = ESMetrics.getInstance();
    this.behaviorMessageParser = BehaviorMessageParser.getInstance();
  }

  /**
   * @param parameters    url parameters
   * @param type          channel type
   * @param action        action type
   * @param request       http request
   * @param uri           url
   * @param channelAction channel action enum
   */
  public void trackUbi(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters, String type,
                String action, HttpServletRequest request, String uri,
                ChannelAction channelAction) {

  }

  /**
   *
   * @param requestContext  request context
   * @param endUserContext  enduserctx header
   * @param referer         referer of the request
   * @param parameters      url parameters
   * @param request         http request
   * @param agentInfo       user agent
   * @param uri             target url
   * @param startTime       start time of the request
   * @param channelType     channel type
   * @param channelAction   action type
   * @param snapshotId      snapshot id
   * @return                behavior message
   */
  public BehaviorMessage parseBehaviorMessage(ContainerRequestContext requestContext, IEndUserContext endUserContext,
                                       String referer, MultiValueMap<String, String> parameters,
                                       HttpServletRequest request, UserAgentInfo agentInfo, String uri,
                                       Long startTime, ChannelType channelType, ChannelAction channelAction,
                                       long snapshotId){
    return behaviorMessageParser.parse(request, requestContext, endUserContext, parameters,
        agentInfo, referer, uri, startTime, channelType, channelAction, snapshotId, 0);
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
    String bu = parameters.get(Constants.BEST_GUESS_USER).get(0);
    Long encryptedUserId = Longs.tryParse(bu);
    if (encryptedUserId != null) {
      requestTracker.addTag("u", String.valueOf(EncryptUtil.decryptUserId(encryptedUserId)), String.class);
    }
  }
}
