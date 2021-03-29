package com.ebay.traffic.chocolate.flink.nrt.provider.ersxid;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ErsXid {
  @JsonProperty("id")
  private String id;
  @JsonProperty("lastSeenTime")
  private long lastSeenTime;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getLastSeenTime() {
    return lastSeenTime;
  }

  public void setLastSeenTime(long lastSeenTime) {
    this.lastSeenTime = lastSeenTime;
  }
}
