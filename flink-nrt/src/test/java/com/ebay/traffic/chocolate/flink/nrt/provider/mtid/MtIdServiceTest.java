/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.mtid;

import com.ebay.traffic.monitoring.ESMetrics;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MtIdServiceTest {

  @Before
  public void setUp() throws Exception {
    ESMetrics.init("test", "http://10.148.181.34:9200");
  }

  @Test
  public void getAccountId() throws Exception {
    Long accountId = MtIdService.getInstance().getAccountId("aac965841710aa412e54755cffff909d","guid").get();
    assertEquals(0, accountId.longValue());
  }
}