package com.ebay.app.raptor.chocolate.adservice.util;

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
