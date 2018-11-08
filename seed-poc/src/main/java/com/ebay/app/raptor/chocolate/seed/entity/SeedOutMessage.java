package com.ebay.app.raptor.chocolate.seed.entity;

/**
 * @author yimeng on 2018/11/7
 */
public class SeedOutMessage {
  private Long timestamp;
  private SeedGroup[] group;

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public SeedGroup[] getGroup() {
    return group;
  }

  public void setGroup(SeedGroup[] group) {
    this.group = group;
  }
}
