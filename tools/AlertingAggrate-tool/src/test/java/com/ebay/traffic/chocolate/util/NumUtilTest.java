package com.ebay.traffic.chocolate.util;

import org.junit.Assert;
import org.junit.Test;

public class NumUtilTest {

  @Test
  public void testParseLong(){
    String num = "1.033574797E9";

    long expected = 1033574848;
    long actual = NumUtil.parseLong(num);

    Assert.assertEquals(actual, expected);
  }



}
