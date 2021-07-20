/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.model;

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;

import java.util.Map;

public class BaseEvent {

  // timestamp
  private long timestamp;

  // unified uuid
  private String uuid;

  // guid
  private String guid;

  // user id
  private String uid;

  // raw incoming body info
  private String url;
  private String referer;
  private EventPayload payload;

  // url parameters
  private UriComponents uriComponents;
  private MultiValueMap<String, String> urlParameters;

  // incoming headers
  private Map<String, String> requestHeaders;

  // channel, action type
  private ChannelIdEnum channelType;
  private ChannelActionEnum actionType;

  // client info
  private UserPrefsCtx userPrefsCtx;
  private UserAgentInfo userAgentInfo;

  // user info
  private IEndUserContext endUserContext;

  // flag of setting timestamp from caller
  private boolean isCheckoutApi;

  // roi event
  private ROIEvent roiEvent;

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getReferer() {
    return referer;
  }

  public void setReferer(String referer) {
    this.referer = referer;
  }

  public Map<String, String> getRequestHeaders() {
    return requestHeaders;
  }

  public void setRequestHeaders(Map<String, String> requestHeaders) {
    this.requestHeaders = requestHeaders;
  }

  public ChannelIdEnum getChannelType() {
    return channelType;
  }

  public void setChannelType(ChannelIdEnum channelType) {
    this.channelType = channelType;
  }

  public ChannelActionEnum getActionType() {
    return actionType;
  }

  public void setActionType(ChannelActionEnum actionType) {
    this.actionType = actionType;
  }

  public UserPrefsCtx getUserPrefsCtx() {
    return userPrefsCtx;
  }

  public void setUserPrefsCtx(UserPrefsCtx userPrefsCtx) {
    this.userPrefsCtx = userPrefsCtx;
  }

  public UserAgentInfo getUserAgentInfo() {
    return userAgentInfo;
  }

  public void setUserAgentInfo(UserAgentInfo userAgentInfo) {
    this.userAgentInfo = userAgentInfo;
  }

  public UriComponents getUriComponents() {
    return uriComponents;
  }

  public void setUriComponents(UriComponents uriComponents) {
    this.uriComponents = uriComponents;
  }

  public MultiValueMap<String, String> getUrlParameters() {
    return urlParameters;
  }

  public void setUrlParameters(MultiValueMap<String, String> urlParameters) {
    this.urlParameters = urlParameters;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public boolean isCheckoutApi() {
    return isCheckoutApi;
  }

  public void setCheckoutApi(boolean checkoutApi) {
    isCheckoutApi = checkoutApi;
  }

  public EventPayload getPayload() {
    return payload;
  }

  public void setPayload(EventPayload payload) {
    this.payload = payload;
  }

  public IEndUserContext getEndUserContext() {
    return endUserContext;
  }

  public void setEndUserContext(IEndUserContext endUserContext) {
    this.endUserContext = endUserContext;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public ROIEvent getRoiEvent() {
    return roiEvent;
  }

  public void setRoiEvent(ROIEvent roiEvent) {
    this.roiEvent = roiEvent;
  }
}
