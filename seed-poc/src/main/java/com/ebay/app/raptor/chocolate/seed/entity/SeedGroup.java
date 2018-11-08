package com.ebay.app.raptor.chocolate.seed.entity;

/**
 * @author yimeng on 2018/11/7
 */
public class SeedGroup {
  private Integer groupId;
  private Float score;
  private String[] items;

  public Integer getGroupId() {
    return groupId;
  }

  public void setGroupId(Integer groupId) {
    this.groupId = groupId;
  }

  public Float getScore() {
    return score;
  }

  public void setScore(Float score) {
    this.score = score;
  }

  public String[] getItems() {
    return items;
  }

  public void setItems(String[] items) {
    this.items = items;
  }
}
