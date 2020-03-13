package com.ebay.app.raptor.chocolate.adservice.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IdMapUrlBuilderTest {
  @Test
  public void hashData() {
    assertEquals("asMqXKToO0clhj5vPZ_grTnPcjyR87ZSdTDh2s51zDg", IdMapUrlBuilder.hashData("1608366025", IdMapUrlBuilder.HASH_ALGO_SHA_256));
    assertNull(IdMapUrlBuilder.hashData("1608366025", "1608366025"));
  }
}