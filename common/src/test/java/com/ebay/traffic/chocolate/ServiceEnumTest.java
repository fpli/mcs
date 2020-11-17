package com.ebay.traffic.chocolate;

import org.junit.Test;

import static org.junit.Assert.*;

public class ServiceEnumTest {

  @Test
  public void fromString() {
    assertEquals(ServiceEnum.DISPATCH, ServiceEnum.fromString(""));
    assertEquals(ServiceEnum.DELSTATS, ServiceEnum.fromString("DELSTATS"));
  }

  @Test
  public void getValue() {
    assertEquals("delstats", ServiceEnum.DELSTATS.getValue());
    assertEquals("CHOCOLATE", ServiceEnum.CHOCOLATE.getValue());
  }
}