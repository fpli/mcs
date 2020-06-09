/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.mtid;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MtIdServiceTest {

  @Test
  public void getAccountId() throws Exception {
    String accountId = MtIdService.getInstance().getAccountId("aac965841710aa412e54755cffff909d","guid").get();
    assertEquals("809423400", accountId);
    System.out.println(accountId);
  }
}