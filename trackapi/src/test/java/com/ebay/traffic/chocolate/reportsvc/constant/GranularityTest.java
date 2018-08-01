package com.ebay.traffic.chocolate.reportsvc.constant;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.Calendar;

public class GranularityTest {

  @Test
  public void testGetGranularityForDifferentIntervalsReturnsCorrectEnum() {
    Assert.assertSame(Granularity.FINE, Granularity.getGranularityForInterval(0));
    Assert.assertSame(Granularity.FINE, Granularity.getGranularityForInterval(1));
    Assert.assertSame(Granularity.DAY, Granularity.getGranularityForInterval(2));
    Assert.assertSame(Granularity.DAY, Granularity.getGranularityForInterval(10));
    Assert.assertSame(Granularity.DAY, Granularity.getGranularityForInterval(14));
    Assert.assertSame(Granularity.WEEK, Granularity.getGranularityForInterval(15));
    Assert.assertSame(Granularity.WEEK, Granularity.getGranularityForInterval(75));
    Assert.assertSame(Granularity.WEEK, Granularity.getGranularityForInterval(90));
    Assert.assertSame(Granularity.MONTH, Granularity.getGranularityForInterval(100));
  }

  @Test
  public void testGetGranularityForOneDayShouldReturnFine() {
    Calendar cal = Calendar.getInstance();

    String start = DateRange.DATE_FORMAT.format(cal.getTime());
    String end = DateRange.DATE_FORMAT.format(cal.getTime());

    Granularity actual = null;
    try {
      actual = Granularity.getGranularityForCustomDateRange(start, end);
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertNotNull(actual);
    Assert.assertSame(Granularity.FINE, actual);
  }

  @Test
  public void testGetGranularityForTenDaysShouldReturnDay() {
    Calendar cal = Calendar.getInstance();

    String start = DateRange.DATE_FORMAT.format(cal.getTime());
    cal.add(Calendar.DAY_OF_MONTH, 10);
    String end = DateRange.DATE_FORMAT.format(cal.getTime());

    Granularity actual = null;
    try {
      actual = Granularity.getGranularityForCustomDateRange(start, end);
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertNotNull(actual);
    Assert.assertSame(Granularity.DAY, actual);
  }

  @Test
  public void testGetGranularityForOneMonthShouldReturnWeek() {
    Calendar cal = Calendar.getInstance();

    String start = DateRange.DATE_FORMAT.format(cal.getTime());
    cal.add(Calendar.MONTH, 1);
    String end = DateRange.DATE_FORMAT.format(cal.getTime());

    Granularity actual = null;
    try {
      actual = Granularity.getGranularityForCustomDateRange(start, end);
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertNotNull(actual);
    Assert.assertSame(Granularity.WEEK, actual);
  }

  @Test
  public void testGetGranularityForOneMonthReversedShouldReturnWeek() {
    Calendar cal = Calendar.getInstance();

    String start = DateRange.DATE_FORMAT.format(cal.getTime());
    cal.add(Calendar.MONTH, 1);
    String end = DateRange.DATE_FORMAT.format(cal.getTime());

    Granularity actual = null;
    try {
      actual = Granularity.getGranularityForCustomDateRange(end, start); // reversed
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertNotNull(actual);
    Assert.assertSame(Granularity.WEEK, actual);
  }

  @Test
  public void testGetGranularityForOneYearShouldReturnMonth() {
    Calendar cal = Calendar.getInstance();

    String start = DateRange.DATE_FORMAT.format(cal.getTime());
    cal.add(Calendar.YEAR, 1);
    String end = DateRange.DATE_FORMAT.format(cal.getTime());

    Granularity actual = null;
    try {
      actual = Granularity.getGranularityForCustomDateRange(start, end);
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertNotNull(actual);
    Assert.assertSame(Granularity.MONTH, actual);
  }
}
