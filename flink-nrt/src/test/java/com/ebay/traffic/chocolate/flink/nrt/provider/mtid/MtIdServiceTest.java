/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.mtid;

import org.junit.Test;

public class MtIdServiceTest {

  @Test
  public void getAccountId() {
    String accountId = MtIdService.getInstance().getAccountId("3a391b4b16f0abc16842c351ebd15330","cguid");
    System.out.println(accountId);
  }
}