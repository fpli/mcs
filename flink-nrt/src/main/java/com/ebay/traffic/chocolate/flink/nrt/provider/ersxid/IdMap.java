package com.ebay.traffic.chocolate.flink.nrt.provider.ersxid;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IdMap {
  @JsonProperty("accounts")
  private List<ErsXid> accounts;

  public List<ErsXid> getAccounts() {
    return accounts;
  }

  public void setAccounts(List<ErsXid> accounts) {
    this.accounts = accounts;
  }
}
