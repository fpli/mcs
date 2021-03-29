package com.ebay.traffic.chocolate.flink.nrt.provider.ersxid;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ErsXidResponse {
  @JsonProperty("idMap")
  private List<IdMap> idMap;

  public List<IdMap> getIdMap() {
    return idMap;
  }

  public void setIdMap(List<IdMap> idMap) {
    this.idMap = idMap;
  }

}
