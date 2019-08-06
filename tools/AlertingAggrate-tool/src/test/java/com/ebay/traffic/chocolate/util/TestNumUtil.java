package com.ebay.traffic.chocolate.util;

import org.junit.Assert;
import org.junit.Test;

public class TestNumUtil {

  @Test
  public void testParseLong() {
    String num = "0.0";

    long expected = 0;
    long actual = NumUtil.parseLong(num);

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetStateWhenAlertIsTrue() {
    long threshold = 100;
    long l2 = 2;
    double thresholdFactor = 0.5;

    String expected = "2";
    String actual = NumUtil.getStateWhenAlertIsTrue(threshold, l2, thresholdFactor);

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetStateWhenAlertIsFalse() {
    long threshold = 100;
    long l2 = 10000;
    double thresholdFactor = 2;

    String expected = "2";
    String actual = NumUtil.getStateWhenAlertIsFalse(threshold, l2, thresholdFactor);

    Assert.assertEquals(actual, expected);
  }

}
