/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate;

public enum ChannelTypeEnum {
  EPN("EPN"),
  PLA("PLA"),
  TEXT("TEXT"),
  PAID_SOCIAL("PAID_SOCIAL"),
  DISPLAY("DISPLAY"),
  SEARCH_ENGINE_FREE_LISTINGS("SEARCH_ENGINE_FREE_LISTINGS"),
  SEO("SEO"),
  SOCIAL_MEDIA("SOCIAL_MEDIA"),
  MRKT_EMAIL("MRKT_EMAIL"),
  SITE_EMAIL("SITE_EMAIL"),
  MRKT_MYMESSAGES("MRKT_MESSAGE_CENTER"),
  SITE_MYMESSAGES("SITE_MESSAGE_CENTER"),
  MOB_NOTIF("MOBILE_NOTIF"),
  SMART("SMART"),
  ONSITE("ONSITE"),
  HUB_NOTIF("HUB_NOTIF"),
  MRKT_SMS("MRKT_SMS"),
  SITE_SMS("SITE_SMS");

  private String mValue;

  ChannelTypeEnum(String value) {
    mValue = value;
  }

  public String getValue() {
    return mValue;
  }

  public static ChannelTypeEnum fromString(String value) {
    if (value != null) {
      for (ChannelTypeEnum flag : ChannelTypeEnum.values()) {
        if (value.equalsIgnoreCase(flag.mValue)) {
          return flag;
        }
      }
    }

    return ChannelTypeEnum.MRKT_EMAIL;
  }
}
