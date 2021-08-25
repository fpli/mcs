/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class CollectionServiceUtilTest {

  @Test
  public void inRefererWhitelist() {
    assertFalse(CollectionServiceUtil.inRefererWhitelist(ChannelType.EPN, "http://www.ebay.com"));
    assertFalse(CollectionServiceUtil.inRefererWhitelist(ChannelType.EPN, "https://ebay.mtag.io/"));
    assertFalse(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, "http://www.ebay.com"));
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, "https://ebay.mtag.io/"));
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, "https://ebay.pissedconsumer.com/"));
    assertFalse(CollectionServiceUtil.inRefererWhitelist(ChannelType.PAID_SEARCH, "https://ebay.pissedconsumer.com/"));

    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, "http://ebay.mtag.io/"));
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, "http://ebay.pissedconsumer.com/"));
  }

}
