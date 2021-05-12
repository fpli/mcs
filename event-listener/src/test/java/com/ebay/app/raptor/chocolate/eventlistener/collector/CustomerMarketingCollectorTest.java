/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = EventListenerApplication.class)
public class CustomerMarketingCollectorTest {

  @Autowired
  CustomerMarketingCollector collector;

  @Test
  public void isVodInternal() {
    String referer = "https://signin.ebay.com/";
    List<String> pathSegments = Arrays.asList("vod", "FetchOrderDetails");
    // not email channel
    assertFalse(collector.isVodInternal(ChannelIdEnum.EPN, pathSegments));

    // email channel success
    assertTrue(collector.isVodInternal(ChannelIdEnum.MRKT_EMAIL, pathSegments));
    assertTrue(collector.isVodInternal(ChannelIdEnum.SITE_EMAIL, pathSegments));

    // path length < 2
    pathSegments = new ArrayList<>();
    assertFalse(collector.isVodInternal(ChannelIdEnum.SITE_EMAIL, pathSegments));
    pathSegments.add("test");
    assertFalse(collector.isVodInternal(ChannelIdEnum.SITE_EMAIL, pathSegments));

    // not vod page
    pathSegments = Arrays.asList("test", "FetchOrderDetails");
    assertFalse(collector.isVodInternal(ChannelIdEnum.SITE_EMAIL, pathSegments));
    pathSegments = Arrays.asList("vod", "test");
    assertFalse(collector.isVodInternal(ChannelIdEnum.SITE_EMAIL, pathSegments));
  }
}