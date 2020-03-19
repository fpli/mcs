package com.ebay.app.raptor.chocolate.eventlistener.util;

/**
 * Define the PageId used in UBI
 *
 * @author Zhiyuan Wang
 * @since 2019/11/25
 */
public enum PageIdEnum {
  // for click
  CLICK(2547208),
  // for email open
  EMAIL_OPEN(3962),
  // for ad request
  AR(2561745),
  // for roi event
  ROI(2483445);

  private final int id;

  PageIdEnum(int pageId) {
    id = pageId;
  }

  public int getId() {
    return id;
  }
}
