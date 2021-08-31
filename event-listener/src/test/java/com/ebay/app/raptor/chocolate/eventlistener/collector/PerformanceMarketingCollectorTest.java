/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.util.SearchEngineFreeListingsRotationEnum;
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.raptor.geo.context.GeoCtx;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = EventListenerApplication.class)
public class PerformanceMarketingCollectorTest {

  @Autowired
  private PerformanceMarketingCollector performanceMarketingCollector;

  @Test
  public void testEraseGDPR() {
    GdprConsentDomain gdprConsentDomain = Mockito.mock(GdprConsentDomain.class);
    when(gdprConsentDomain.isAllowedStoredContextualData()).thenReturn(false);
    when(gdprConsentDomain.isAllowedStoredPersonalizedData()).thenReturn(false);
    ListenerMessage message = new ListenerMessage();
    performanceMarketingCollector.eraseByGdpr(gdprConsentDomain, message);
    assertEquals("", message.getRemoteIp());
    assertEquals(0L, message.getUserId().longValue());

  }

  @Test
  public void testGetSearchEngineFreeListingsRotationId() {
    UserPrefsCtx mockUserPrefsCtx = Mockito.mock(UserPrefsCtx.class);
    GeoCtx mockGeoCtx = Mockito.mock(GeoCtx.class);
    when(mockUserPrefsCtx.getGeoContext()).thenReturn(mockGeoCtx);
    when(mockGeoCtx.getSiteId()).thenReturn(0);
    assertEquals(SearchEngineFreeListingsRotationEnum.US.getRotation(),
        performanceMarketingCollector.getSearchEngineFreeListingsRotationId(mockUserPrefsCtx));
  }

  @Test
  public void setCheckoutApiFlag() {
    BaseEvent baseEvent = new BaseEvent();
    long currentTs = System.currentTimeMillis();
    long checkoutTs = currentTs + 10;
    baseEvent.setTimestamp(currentTs);
    baseEvent.setChannelType(ChannelIdEnum.EPN);
    EventPayload payload = new EventPayload();
    baseEvent.setPayload(payload);
    // exception setting timestamp
    payload.setCheckoutAPIClickTs("abcdef");
    performanceMarketingCollector.setCheckoutTimestamp(baseEvent);
    assertEquals(currentTs, baseEvent.getTimestamp());

    // successful set timestamp
    payload.setCheckoutAPIClickTs(String.valueOf(checkoutTs));
    performanceMarketingCollector.setCheckoutTimestamp(baseEvent);
    assertEquals(checkoutTs, baseEvent.getTimestamp());
  }
}
