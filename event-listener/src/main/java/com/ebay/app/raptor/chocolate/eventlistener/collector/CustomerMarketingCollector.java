/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * @author xiangli4
 * Track customer marketing channels
 * 1. Ubi message,
 * 2. Behavior message
 */
public interface CustomerMarketingCollector {
  public void trackUbi(ContainerRequestContext requestContext, IEndUserContext endUserContext,
                       String referer, MultiValueMap<String, String> parameters, String type,
                       String action, HttpServletRequest request, UserAgentInfo agentInfo, String uri,
                       Long startTime, ChannelType channelType, ChannelAction channelAction, boolean isDuplicateClick);

  public BehaviorMessage parseBehaviorMessage(ContainerRequestContext requestContext, IEndUserContext endUserContext,
                                              String referer, MultiValueMap<String, String> parameters, String type,
                                              String action, HttpServletRequest request, UserAgentInfo agentInfo,
                                              String uri, Long startTime, ChannelType channelType,
                                              ChannelAction channelAction, long snapshotId, boolean isDuplicateClick);
}
