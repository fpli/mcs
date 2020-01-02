/*
 * Copyright (c) 2019. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.adservice.constant;

/**
 * @author Zhiyuan Wang
 * @since 2019/10/30
 */
public enum MKEVT {
  MARKETING_EVENT(1),
  IMPRESSION(2),
  VIEWABLE_IMPRESSION(3),
  EMAIL_OPEN(4),
  NOTIFICATION(5),
  AD_REQUEST(6);

  private final int id;

  MKEVT(final int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
