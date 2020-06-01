package com.ebay.traffic.chocolate.flink.nrt.util;

public enum PropertyEnv {
  DEV("DEV"),
  PROD("PROD");

  private final String name;

  PropertyEnv(final String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }
}
