package com.ebay.traffic.chocolate.flink.nrt.constant;

public enum RvrCmndTypeCdEnum {
  IMPRESSION("4"),
  ROI("2"),
  SERVE("4"),
  CLICK("1");

  private final String cd;

  RvrCmndTypeCdEnum(final String id) {
    this.cd = id;
  }

  public String getCd() {
    return this.cd;
  }
}
