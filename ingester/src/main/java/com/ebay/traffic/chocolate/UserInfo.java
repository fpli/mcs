package com.ebay.traffic.chocolate;

public class UserInfo
{
  private String user_id;
  private Long activity_ts;
  
  public String getUser_id()
  {
    return this.user_id;
  }
  
  public void setUser_id(String user_id)
  {
    this.user_id = user_id;
  }
  
  public Long getActivity_ts()
  {
    return this.activity_ts;
  }
  
  public void setActivity_ts(Long activity_ts)
  {
    this.activity_ts = activity_ts;
  }
}
