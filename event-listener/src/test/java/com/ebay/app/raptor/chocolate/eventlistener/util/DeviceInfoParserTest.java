package com.ebay.app.raptor.chocolate.eventlistener.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by jialili1 on 11/23/20
 */
public class DeviceInfoParserTest {

  @Test
  public void testParser() {

    // app agent
    DeviceInfoParser parser = new DeviceInfoParser("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;" +
        "no-carrier;414x736;3.0").parse();

    assertEquals("Other", parser.getDeviceFamily());
    assertEquals("iOS", parser.getDeviceType());
    assertNull(parser.getBrowserFamily());
    assertNull(parser.getBrowserVersion());
    assertEquals("iOS", parser.getOsFamily());
    assertEquals("11.2", parser.getOsVersion());
  }
}
