package com.ebay.traffic.chocolate.flink.nrt.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class PropertyEnvTest {

  @Test
  public void getName() {
    assertEquals("DEV", PropertyEnv.DEV.getName());
  }

  @Test
  public void values() {
    assertArrayEquals(new PropertyEnv[]{PropertyEnv.DEV, PropertyEnv.PROD, PropertyEnv.STAGING}, PropertyEnv.values());
  }

  @Test
  public void valueOf() {
    assertEquals(PropertyEnv.DEV, PropertyEnv.valueOf("DEV"));
  }
}