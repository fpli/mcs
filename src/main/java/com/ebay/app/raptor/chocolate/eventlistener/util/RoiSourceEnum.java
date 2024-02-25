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
  // for Checkout API
  CHECKOUT_SOURCE(2),
  // for ad request
  APP_SOURCE(3),
  // for the Roi Source which call MCS directly
  DIRECT_SOURCE(4),
  // for batchtrack
  BATCHTRACK_SOURCE(5),
  // for PlaceOffer API
  PLACEOFFER_SOURCE(6);

  private final int id;

  RoiSourceEnum(int sourceId) {
    id = sourceId;
  }

  public int getId() {
    return id;
  }
}
