/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.utp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RecoList{
  @JsonProperty("reco.type")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String recoType;
  @JsonProperty("reco.id")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String recoId;
  @JsonProperty("reco.pos")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String recoPos;
}
