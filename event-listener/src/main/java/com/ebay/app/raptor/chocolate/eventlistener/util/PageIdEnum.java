package com.ebay.app.raptor.chocolate.eventlistener.util;

/**
 * Define the PageId used in UBI
 *
 * @author Zhiyuan Wang
 * @since 2019/11/25
 */
public enum PageIdEnum {
  // for click
  CLICK(2547208, "event"),
  // for email open
  EMAIL_OPEN(3962, "impression"),
  // for ad request
  AR(2561745, "ar"),
  // for roi event
  ROI(2483445, "roi"),
  // Notification Received
  NOTIFICATION_RECEIVED(2054081, "notificationReceived"),
  // Notification Action
  NOTIFICATION_ACTION(2054060, "notificationAction");

  private final int id;
  private final String name;

  PageIdEnum(int pageId, String pageName) {
    id = pageId;
    name = pageName;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }
}
