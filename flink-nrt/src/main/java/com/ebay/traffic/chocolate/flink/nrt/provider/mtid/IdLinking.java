/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.mtid;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * ID linking return format
 *
 * {
 *   "idLinking": [
 *     {
 *       "ids": [
 *         "35646794"
 *       ],
 *       "type": "account"
 *     },
 *     {
 *       "ids": [
 *         "5276c0b711d0a18680d6ea26fffffc76"
 *       ],
 *       "type": "guid"
 *     },
 *     {
 *       "ids": [
 *         "5a34fc7111e0a18681a66686fffffe79"
 *       ],
 *       "type": "cguid"
 *     }
 *   ]
 * }
 *
 *  @author xiangli4
 */

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IdLinking {

  @JsonIgnore
  private List<Id> idList;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("idLinking")
  public List<Id> getIdList() {
    return idList;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("idLinking")
  public void setIdList(List<Id> idList) {
    this.idList = idList;
  }

  @JsonIgnore
  public void addIdList(List<Id> idList) {
    if(this.idList == null) {
      this.idList = idList;
    } else {
      this.idList.addAll(idList);
    }
  }
}
