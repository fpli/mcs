package com.ebay.traffic.chocolate;

public class Group
{
  private String groupId;
  private Float score;
  private String[] items;
  
  public String getGroupId()
  {
    return this.groupId;
  }
  
  public void setGroupId(String groupId)
  {
    this.groupId = groupId;
  }
  
  public Float getScore()
  {
    return this.score;
  }
  
  public void setScore(Float score)
  {
    this.score = score;
  }
  
  public String[] getItems()
  {
    return this.items;
  }
  
  public void setItems(String[] items)
  {
    this.items = items;
  }
}
