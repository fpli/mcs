package com.ebay.traffic.chocolate.flink.nrt.util;

/**
 * Represents the environment in which the current application is running.
 *
 * @author Zhiyuan Wang
 * @since 2020/9/14
 */
public enum PropertyEnv {
  DEV("DEV"),
  PROD("PROD"),
  STAGING("STAGING");

  private final String name;

  PropertyEnv(final String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }
}
