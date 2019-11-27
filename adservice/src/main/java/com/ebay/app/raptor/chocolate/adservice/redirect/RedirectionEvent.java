package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;

/**
 * Redirection event information
 *
 * Created by jialili1 on 11/21/19
 */
public class RedirectionEvent {
  private String redirectUrl;
  private String redirectSource;
  private String partnerId;
  private String channelId;
  private String channelType;
  private String actionId;

  private static final String ADOBE = "adobe";

  public RedirectionEvent(String channelId, String actionId, String partnerId) {
    this.channelId = channelId;
    this.channelType = ChannelIdEnum.parse(channelId).getLogicalChannel().getAvro().toString();
    this.actionId = actionId;
    this.partnerId = partnerId;
  }

  public void setRedirectUrl(String redirectUrl) {
    this.redirectUrl = redirectUrl;
  }

  public String getRedirectUrl() {
    return redirectUrl;
  }

  public void setRedirectSource(String redirectSource) {
    this.redirectSource = redirectSource;
  }

  public String getRedirectSource() {
    return redirectSource;
  }

  public void setPartnerId(String partnerId) {
    this.partnerId = partnerId;
  }

  public String getPartnerId() {
    return partnerId;
  }

  public String getPartner() {
    if (Constants.ADOBE_PARTNER_ID.equals(partnerId)) {
      return ADOBE;
    } else {
      return null;
    }
  }

  public void setChannelId(String channelId) {
    this.channelId = channelId;
  }

  public String getChannelId() {
    return channelId;
  }

  public void setChannelType(String channel) {
    this.channelType = channel;
  }

  public String getChannelType() {
    return channelType;
  }

  public void setActionId(String actionId) {
    this.actionId = actionId;
  }

  public String getActionId() {
    return actionId;
  }
}
