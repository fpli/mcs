package com.ebay.traffic.chocolate;

import org.junit.Test;

import static org.junit.Assert.*;

public class MessageConstantsEnumTest {

  @Test
  public void getValue() {
    assertEquals("creative.variation.id", MessageConstantsEnum.CREATIVE_VARIATION_ID.getValue());
  }
}