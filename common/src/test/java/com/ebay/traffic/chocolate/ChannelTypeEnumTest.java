package com.ebay.traffic.chocolate;

import org.junit.Test;

import static org.junit.Assert.*;

public class ChannelTypeEnumTest {

  @Test
  public void fromString() {
    assertEquals(ChannelTypeEnum.MRKT_EMAIL, ChannelTypeEnum.fromString(""));
    assertEquals(ChannelTypeEnum.MOB_NOTIF, ChannelTypeEnum.fromString("MOBILE_NOTIF"));
  }

  @Test
  public void getValue() {
    assertEquals("MOBILE_NOTIF", ChannelTypeEnum.MOB_NOTIF.getValue());
  }
}