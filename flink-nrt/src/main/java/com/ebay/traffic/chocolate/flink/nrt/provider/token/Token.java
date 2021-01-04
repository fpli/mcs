/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.token;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Token {

  @JsonIgnore
  private String token;

  @JsonIgnore
  private Integer expires;

  @JsonIgnore
  private String tokenType;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("access_token")
  public String getToken() {
    return token;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("access_token")
  public void setToken(String token) {
    this.token = token;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("expires_in")
  public Integer getExpires() {
    return expires;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("expires_in")
  public void setExpires(Integer expires) {
    this.expires = expires;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("token_type")
  public String getTokenType() {
    return tokenType;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("token_type")
  public void setTokenType(String tokenType) {
    this.tokenType = tokenType;
  }
}
