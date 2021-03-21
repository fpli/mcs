package com.ebay.traffic.chocolate.flink.nrt.constant;

public enum RvrChnlTypeCdEnum {
  EPN("1"),
  DISPLAY("4"),
  PAID_SEARCH("2"),
  SOCIAL_MEDIA("16"),
  PAID_SOCIAL("20"),
  ROI("0"),
  NATURAL_SEARCH("3"),
  SEARCH_ENGINE_FREE_LISTINGS("28"),
  DEFAULT("0");

  private final String cd;

  RvrChnlTypeCdEnum(final String id) {
    this.cd = id;
  }

  public String getCd() {
    return this.cd;
  }
}
