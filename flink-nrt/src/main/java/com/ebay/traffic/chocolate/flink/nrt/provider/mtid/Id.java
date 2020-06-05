/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.mtid;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Id format
 * @author xiangli4
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Id {

  @JsonIgnore
  private List<String> ids;

  @JsonIgnore
  private String type;

//  @JsonIgnore
//  public Id(String type) {
//    this.type = type;
//  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("ids")
  public List<String> getIds() {
    return ids;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("ids")
  public void setIds(List<String> ids) {
    this.ids = ids;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }
}
