package com.ebay.app.raptor.chocolate.constant;

/**
 * Identify the various marketing channels for which we track.
 */
public enum MPLXChannelEnum {
  DISPLAY(1, "Display", 4),
  PAID_SEARCH(2,"Paid Search",2),
  NATUAL_SEARCH(3,"Natural Search",3),
  EPN(6,"Affiliate",1),
  SOCIAL_MEDIA(13,"Social Media",16),
  PAID_SOCIAL(33,"Paid Social",20);


  private Integer mplxChannelId;
  private String mplxChannelName;
  private Integer roverChannelId;

  public Integer getMplxChannelId() {
    return mplxChannelId;
  }

  public String getMplxChannelName() {
    return mplxChannelName;
  }

  public Integer getRoverChannelId() {
    return roverChannelId;
  }

  MPLXChannelEnum(Integer mplxChannelId, String mplxChannelName, Integer roverChannelId) {
    this.mplxChannelId = mplxChannelId;
    this.mplxChannelName = mplxChannelName;
    this.roverChannelId = roverChannelId;
  }

  public static MPLXChannelEnum getByRoverChannelId(int roverChannelId) {
    for (MPLXChannelEnum mplxChannelEnum : MPLXChannelEnum.values()) {
      if (mplxChannelEnum.roverChannelId == roverChannelId) {
        return mplxChannelEnum;
      }
    }
    return null;
  }
}
