package com.ebay.app.raptor.chocolate.util;

import com.ebay.app.raptor.chocolate.adservice.util.HttpUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by jialili1 on 2/26/21
 */
public class HttpUtilTest {
  private static final String guid = "11111111111111111111111111111111";
  private static final String adguid = "22222222222222222222222222222222";

  @Test
  public void constructTrackingHeader() {
    String header = HttpUtil.constructTrackingHeader("", "");
    assertEquals("", header);

    header = HttpUtil.constructTrackingHeader(guid, "");
    assertEquals("guid=11111111111111111111111111111111", header);

    header = HttpUtil.constructTrackingHeader("", adguid);
    assertEquals("adguid=22222222222222222222222222222222", header);

    header = HttpUtil.constructTrackingHeader(guid, adguid);
    assertEquals("guid=11111111111111111111111111111111,adguid=22222222222222222222222222222222", header);
  }
}
