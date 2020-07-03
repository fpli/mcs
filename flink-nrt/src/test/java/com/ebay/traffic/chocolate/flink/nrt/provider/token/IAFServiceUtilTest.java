/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.token;

import org.junit.Test;

import static org.junit.Assert.*;

public class IAFServiceUtilTest {

  @Test
  public void getAppToken() {
    String token = IAFServiceUtil.getInstance().getAppToken();
    assertNotNull(token);
  }
}