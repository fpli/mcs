package com.ebay.traffic.chocolate.util;

public enum ChannelType {

  ROI("ROI", 0), PAID_SEARCH("Paid Search", 2), NATRUAL_SEARCH("Natrual Search", 3), DISPLAY("Display", 4), SOCIAL_MEDIA("Social Media", 16);

  private String name;
  private int index;

  private ChannelType(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

}
