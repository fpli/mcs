package com.ebay.app.raptor.chocolate.eventlistener.util;

/**
 * Define the roisrc value
 *
 * @author Zhiyuan Wang
 * @since 2020/03/19
 */
public enum RoiSourceEnum {
  // Web/mWeb source(BESConsumer)
  BES_SOURCE(1),
  // for email open
  CHECKOUT_SOURCE(2),
  // for ad request
  APP_SOURCE(3);

  private final int id;

  RoiSourceEnum(int sourceId) {
    id = sourceId;
  }

  public int getId() {
    return id;
  }
}
