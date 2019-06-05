package com.ebay.traffic.chocolate.util;

import org.junit.Assert;
import org.junit.Test;

public class NumUtilTest {

  @Test
  public void testParseLong() {
    String num = "1.033574797E9";

    long expected = 1033574848;
    long actual = NumUtil.parseLong(num);

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetStateWhenAlertIsTrue() {
    long threshold = 100;
    long l2 = 2;

    String expected = "2";
    String actual = NumUtil.getStateWhenAlertIsTrue(threshold, l2);

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetStateWhenAlertIsFalse() {
    long threshold = 100;
    long l2 = 10000;

    String expected = "2";
    String actual = NumUtil.getStateWhenAlertIsFalse(threshold, l2);

    Assert.assertEquals(actual, expected);
  }

}
