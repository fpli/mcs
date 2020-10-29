/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate;

public enum ActionTypeEnum {
  OPEN("OPEN"),
  CLICK("CLICK"),
  HOVER("HOVER"),
  PULL("PULL"),
  UNSUB("UNSUB"),
  SEND("SEND"),
  SPAM("SPAM"),
  BOUNCE("BOUNCE"),
  DISMISS("DISMISS"),
  DELIVERED("DELIVERED"),
  IMPRESSION("IMPRESSION"),
  SERVE("SERVE"),
  ROI("ROI"),
  NOTIFICATION_RECEIVED("NOTIFICATION_RECEIVED"),
  NOTIFICATION_ACTION("NOTIFICATION_ACTION");

  private final String mValue;

  ActionTypeEnum(String value) {
    mValue = value;
  }

  public String getValue() {
    return mValue;
  }
}
