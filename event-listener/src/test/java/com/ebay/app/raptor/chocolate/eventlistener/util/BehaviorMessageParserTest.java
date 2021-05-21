/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.container.ContainerRequestContext;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class BehaviorMessageParserTest {

  @BeforeClass
  public static void beforeTest() {
    BehaviorMessageParser.init();
  }

  @Test
  public void getDeviceFamily() {
    DDSResponse ddsResponse = Mockito.mock(DDSResponse.class);

    when(ddsResponse.isTablet()).thenReturn(true);
    assertEquals("Tablet", BehaviorMessageParser.getInstance().getDeviceFamily(ddsResponse));

    when(ddsResponse.isTablet()).thenReturn(false);
    when(ddsResponse.isTouchScreen()).thenReturn(true);
    assertEquals("TouchScreen", BehaviorMessageParser.getInstance().getDeviceFamily(ddsResponse));

    when(ddsResponse.isTablet()).thenReturn(false);
    when(ddsResponse.isTouchScreen()).thenReturn(false);
    when(ddsResponse.isDesktop()).thenReturn(true);
    assertEquals("Desktop", BehaviorMessageParser.getInstance().getDeviceFamily(ddsResponse));

    when(ddsResponse.isTablet()).thenReturn(false);
    when(ddsResponse.isTouchScreen()).thenReturn(false);
    when(ddsResponse.isDesktop()).thenReturn(false);
    when(ddsResponse.isMobile()).thenReturn(true);
    assertEquals("Mobile", BehaviorMessageParser.getInstance().getDeviceFamily(ddsResponse));

    when(ddsResponse.isTablet()).thenReturn(false);
    when(ddsResponse.isTouchScreen()).thenReturn(false);
    when(ddsResponse.isDesktop()).thenReturn(false);
    when(ddsResponse.isMobile()).thenReturn(false);
    assertEquals("Other", BehaviorMessageParser.getInstance().getDeviceFamily(ddsResponse));

  }

  @Test
  public void removeBsParam() {
    MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
    parameters.add("choco_bs", "123");
    String url = "https://www.ebay.com/?mkevt=1&choco_bs=123";
    String newUrl = BehaviorMessageParser.getInstance().removeBsParam(parameters, url);
    assertEquals("https://www.ebay.com/?mkevt=1", newUrl);
  }

  @Test
  public void testFacebookPrefetch() {
    ContainerRequestContext mockContext = Mockito.mock(ContainerRequestContext.class);
    when(mockContext.getHeaderString("X-Purpose")).thenReturn("preview");
    assertTrue(BehaviorMessageParser.getInstance().isFacebookPrefetchEnabled(mockContext));
  }
}