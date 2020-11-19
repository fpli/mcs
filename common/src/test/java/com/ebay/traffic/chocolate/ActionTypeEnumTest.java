package com.ebay.traffic.chocolate;

import org.junit.Test;

import static org.junit.Assert.*;

public class ActionTypeEnumTest {

  @Test
  public void getValue() {
    assertEquals("OPEN", ActionTypeEnum.OPEN.getValue());
  }
}