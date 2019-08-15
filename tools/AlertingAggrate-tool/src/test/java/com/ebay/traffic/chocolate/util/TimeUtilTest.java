package com.ebay.traffic.chocolate.util;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

public class TimeUtilTest {

//  @Test
//  public void testGetToday(){
//    String expected = "2019.04.03";
//    String actual = TimeUtil.getToday();
//
//    Assert.assertEquals(actual, expected);
//  }

  @Test
  public void testGetHour(){
    String expected = "2019-04-03 10:00:00";
    String actual = TimeUtil.getHour(1554257431460l);

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetTimestamp() throws ParseException {
    long expected = 1554256800000l;
    long actual = TimeUtil.getTimestamp("2019-04-03 10:00:00");

    Assert.assertEquals(actual, expected);
  }

}
