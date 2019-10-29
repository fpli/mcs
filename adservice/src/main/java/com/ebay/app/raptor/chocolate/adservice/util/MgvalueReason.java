package com.ebay.app.raptor.chocolate.adservice.util;

/**
 * @author Zhiyuan Wang
 * @since 2019/10/27
 */
public enum MgvalueReason {
  SUCCESS("SUCCESS", 0),
  NEW_USER("NEW_USER", 1),
  IDLINK_CALL_ERROR("IDLINK_CALL_ERROR", 2),
  TRUST_CALL_ERROR("TRUST_CALL_ERROR", 3),
  BOT("BOT", 4),
  TRACKING_ERROR("TRACKING_ERROR", 5);

  private final String value;

  private final int id;

  private MgvalueReason(final String value, final int id) {
    this.value = value;
    this.id = id;
  }

  public String getValue() { return value; }

  public int getId() {
    return id;
  }
}
