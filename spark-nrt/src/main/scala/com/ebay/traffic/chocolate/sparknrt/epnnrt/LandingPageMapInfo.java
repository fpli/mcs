package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import java.io.Serializable;

// copy from old EPN code
public class LandingPageMapInfo implements Serializable {
  private String AMS_PAGE_TYPE_MAP_ID;
  private String AMS_PRGRM_ID;
  private String LNDNG_PAGE_URL_TXT;

  public String getAMS_PAGE_TYPE_MAP_ID() {
    return AMS_PAGE_TYPE_MAP_ID;
  }

  public String getAMS_PRGRM_ID() {
    return AMS_PRGRM_ID;
  }

  public String getLNDNG_PAGE_URL_TXT() {
    return LNDNG_PAGE_URL_TXT;
  }

  public void setAMS_PAGE_TYPE_MAP_ID(String AMS_PAGE_TYPE_MAP_ID) {
    this.AMS_PAGE_TYPE_MAP_ID = AMS_PAGE_TYPE_MAP_ID;
  }

  public void setAMS_PRGRM_ID(String AMS_PRGRM_ID) {
    this.AMS_PRGRM_ID = AMS_PRGRM_ID;
  }

  public void setLNDNG_PAGE_URL_TXT(String LNDNG_PAGE_URL_TXT) {
    this.LNDNG_PAGE_URL_TXT = LNDNG_PAGE_URL_TXT;
  }
}
