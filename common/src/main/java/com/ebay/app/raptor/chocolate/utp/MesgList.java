/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.utp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.*;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class MesgList{
  @JsonProperty("reco.list")
  @JsonInclude(Include.NON_NULL)
  public List<RecoList> recoList;
  @JsonProperty("mesg.reco.type")
  @JsonInclude(Include.NON_NULL)
  public String mesgRecoType;
  @JsonProperty("mesg.id")
  @JsonInclude(Include.NON_NULL)
  public String mesgId;
  @JsonProperty("mesg.instance.id")
  @JsonInclude(Include.NON_NULL)
  public String mesgInstanceId;
  @JsonProperty("plmt.id")
  @JsonInclude(Include.NON_NULL)
  public String plmtId;
  @JsonProperty("mob.trk.id")
  @JsonInclude(Include.NON_NULL)
  public String mobTrkId;
  @JsonInclude(Include.NON_NULL)
  public String version;
  @JsonProperty("plmt.pos")
  @JsonInclude(Include.NON_NULL)
  public String plmtPos;
}
