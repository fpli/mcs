package com.ebay.traffic.chocolate.reportsvc.constant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.text.ParseException;
import java.util.*;

public class DateRangeTest {

  private Calendar testCal = getTestCalendar(); // 1990/7/4

  @Before
  public void setup() {
    DateRange.calendar = testCal;
  }

  @Test
  public void testGetMonthsForDateRangeForSameStartAndEndDates() throws ParseException {
    List<Integer> expectedMonths = new ArrayList<>();
    Calendar cal = Calendar.getInstance();
    expectedMonths.add(Integer.valueOf(DateRange.MONTH_FORMAT.format(cal.getTime())));
    String date = DateRange.DATE_FORMAT.format(cal.getTime());

    List<Integer> actualMonths = DateRange.getMonthsForDateRange(date, date);

    Assert.assertNotNull(actualMonths);
    Assert.assertFalse(actualMonths.isEmpty());
    Assert.assertEquals(actualMonths.size(), expectedMonths.size());
    Assert.assertEquals(actualMonths.get(0), expectedMonths.get(0));
  }

  @Test
  public void testGetMonthsForDateRangeSixMonthsApart() throws ParseException {
    List<Integer> expectedMonths = new ArrayList<>();
    getTestMonthsList(expectedMonths, 6);

    Calendar cal = Calendar.getInstance();
    String end = DateRange.DATE_FORMAT.format(cal.getTime());
    cal.add(Calendar.MONTH, -6);

    List<Integer> actualMonths = DateRange.getMonthsForDateRange(DateRange.DATE_FORMAT.format(cal.getTime()), end);

    Assert.assertNotNull(actualMonths);
    Assert.assertFalse(actualMonths.isEmpty());
    Assert.assertEquals(actualMonths.size(), expectedMonths.size());

    for (Integer month : actualMonths) {
      Assert.assertTrue(expectedMonths.contains(month));
    }
  }

  @Test
  public void testGetMonthsForDateRangeSixMonthsApartInReverse() throws ParseException {
    List<Integer> expectedMonths = new ArrayList<>();
    getTestMonthsList(expectedMonths, 6);

    Calendar cal = Calendar.getInstance();
    String start = DateRange.DATE_FORMAT.format(cal.getTime());
    cal.add(Calendar.MONTH, -6);

    List<Integer> actualMonths = DateRange.getMonthsForDateRange(start, DateRange.DATE_FORMAT.format(cal.getTime()));

    Assert.assertNotNull(actualMonths);
    Assert.assertFalse(actualMonths.isEmpty());
    Assert.assertEquals(actualMonths.size(), expectedMonths.size());

    for (Integer month : actualMonths) {
      Assert.assertTrue(expectedMonths.contains(month));
    }
  }

  @Test
  public void testGetDatesLastMonth() {
    String[] dateRange = DateRange.getDates(DateRange.LAST_MONTH);
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900601");
    Assert.assertEquals(dateRange[1], "19900630");
  }

  @Test
  public void testGetDatesLastMonth2() throws ParseException {
    List<Integer> months = DateRange.getMonthsForDateRange("20171001", "20171031");
    Assert.assertEquals(1, months.size());
    Assert.assertEquals(201710, months.get(0).intValue());
  }

  @Test
  public void testGetDatesYearToDate() {
    String[] dateRange = DateRange.getDates(DateRange.YEAR_TO_DATE);
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900101");
    Assert.assertEquals(dateRange[1], "19900704");
  }

  @Test
  public void testGetDatesLastWeek() {
    String[] dateRange = DateRange.getDates(DateRange.LAST_WEEK);
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900625");
    Assert.assertEquals(dateRange[1], "19900701");
  }

  @Test
  public void testGetDatesWeekToDate() {
    String[] dateRange = DateRange.getDates(DateRange.WEEK_TO_DATE);
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900702");
    Assert.assertEquals(dateRange[1], "19900704");
  }

  @Test
  public void testGetDatesYesterday() {
    String[] dateRange = DateRange.getDates(DateRange.YESTERDAY);
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900703");
    Assert.assertEquals(dateRange[1], "19900703");
  }

  @Test
  public void testGetDatesToday() {
    String[] dateRange = DateRange.getDates(DateRange.TODAY);
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900704");
    Assert.assertEquals(dateRange[1], "19900704");
  }

  @Test
  public void testGetDatesForCustom() {
    String[] dateRange = DateRange.getDates(DateRange.CUSTOM);
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNull(dateRange[0]);
    Assert.assertNull(dateRange[1]);
  }

  @Test
  public void testTodaysDate() {
    String formattedDate = DateRange.todaysDate();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900704");
  }

  @Test
  public void testYesterdaysDate() {
    String formattedDate = DateRange.yesterdaysDate();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900703");
  }

  @Test
  public void testRangeThisWeek() {
    String[] dateRange = DateRange.rangeThisWeek();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900702");
    Assert.assertEquals(dateRange[1], "19900704");
  }

  @Test
  public void testRangeLastWeek() {
    String[] dateRange = DateRange.rangeLastWeek();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900625");
    Assert.assertEquals(dateRange[1], "19900701");
  }

  @Test
  public void testRangeMonthToDate() {
    String[] dateRange = DateRange.rangeMonthToDate();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900701");
    Assert.assertEquals(dateRange[1], "19900704");
  }

  @Test
  public void testRangeLastMonth() {
    String[] dateRange = DateRange.rangeLastMonth();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900601");
    Assert.assertEquals(dateRange[1], "19900630");
  }

  @Test
  public void testRangeQuarterToDate() {
    String[] dateRange = DateRange.rangeQuarterToDate();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900701");
    Assert.assertEquals(dateRange[1], "19900704");
  }

  @Test
  public void testRangeLastQuarter() {
    String[] dateRange = DateRange.rangeLastQuarter();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900401");
    Assert.assertEquals(dateRange[1], "19900630");
  }

  @Test
  public void testRangeYearToDate() {
    String[] dateRange = DateRange.rangeYearToDate();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19900101");
    Assert.assertEquals(dateRange[1], "19900704");
  }

  @Test
  public void testRangeLastYear() {
    String[] dateRange = DateRange.rangeLastYear();
    Assert.assertNotNull(dateRange);
    Assert.assertEquals(2, dateRange.length);
    Assert.assertNotNull(dateRange[0]);
    Assert.assertNotNull(dateRange[1]);
    Assert.assertEquals(dateRange[0], "19890101");
    Assert.assertEquals(dateRange[1], "19891231");
  }

  @Test
  public void testFormattedDateWithNullReturnsNull() {
    String formattedDate = DateRange.formattedDate(null);
    Assert.assertNull(formattedDate);
  }

  @Test
  public void testFormattedDateWithValidDateFormatsDateAsExpected() {
    String formattedDate = DateRange.formattedDate(testCal.getTime());
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900704");
  }

  @Test
  public void testFormattedMonthWithNullReturnsNull() {
    String formattedMonth = DateRange.formattedMonth(null);
    Assert.assertNull(formattedMonth);
  }

  @Test
  public void testFormattedMonthWithValidDateFormatsMonthAsExpected() {
    String formattedMonth = DateRange.formattedMonth(testCal.getTime());
    Assert.assertNotNull(formattedMonth);
    Assert.assertFalse(formattedMonth.isEmpty());
    Assert.assertEquals(formattedMonth, "199007");
  }

  @Test
  public void testFormatRequestDateWithNullReturnsNull() throws ParseException {
    String formattedDate = DateRange.formatRequestDate(null);
    Assert.assertNull(formattedDate);
  }

  @Test
  public void testConvertDateToRequestFormatWithNullReturnsEmptyString() throws ParseException {
    String actualDate = DateRange.convertDateToRequestFormat(null);
    Assert.assertNotNull(actualDate);
    Assert.assertTrue(actualDate.isEmpty());
  }

  @Test
  public void testConvertDateToRequestFormatWithValidDateFormatsDateAsExpected() throws ParseException {
    String formattedDate = "19900704";
    String convertedDate = "1990-07-04";
    String actualDate = DateRange.convertDateToRequestFormat(formattedDate);
    Assert.assertNotNull(actualDate);
    Assert.assertFalse(actualDate.isEmpty());
    Assert.assertEquals(actualDate, convertedDate);
  }

  @Test
  public void testConvertMonthToRequestFormatWithNullReturnsEmptyString() throws ParseException {
    String actualMonth = DateRange.convertMonthToRequestFormat(null);
    Assert.assertNotNull(actualMonth);
    Assert.assertTrue(actualMonth.isEmpty());
  }

  @Test
  public void testConvertMonthToRequestFormatWithValidDateFormatsDateAsExpected() throws ParseException {
    String formattedMonth = "199007";
    String convertedMonth = "1990-07-01";
    String actualMonth = DateRange.convertMonthToRequestFormat(formattedMonth);
    Assert.assertNotNull(actualMonth);
    Assert.assertFalse(actualMonth.isEmpty());
    Assert.assertEquals(actualMonth, convertedMonth);
  }

  @Test
  public void testGetWeeksForMonthReturnsCorrectSetOfWeeks() throws ParseException {
    int month = 199007;
    String[] actualWeeks = DateRange.getWeeksForMonth(month);
    Assert.assertNotNull(actualWeeks);
    Assert.assertEquals(6, actualWeeks.length);
    Assert.assertEquals("19900625", actualWeeks[0]);
    Assert.assertEquals("19900702", actualWeeks[1]);
    Assert.assertEquals("19900709", actualWeeks[2]);
    Assert.assertEquals("19900716", actualWeeks[3]);
    Assert.assertEquals("19900723", actualWeeks[4]);
    Assert.assertEquals("19900730", actualWeeks[5]);
  }

  @Test
  public void testDetermineWeekForGivenDayIsCorrect() throws ParseException {
    Assert.assertEquals(19900625, DateRange.determineWeekForDay(19900701));
    Assert.assertEquals(19900702, DateRange.determineWeekForDay(19900702));
    Assert.assertEquals(19900709, DateRange.determineWeekForDay(19900710));
    Assert.assertEquals(19900716, DateRange.determineWeekForDay(19900719));
    Assert.assertEquals(19900723, DateRange.determineWeekForDay(19900728));
    Assert.assertEquals(19900730, DateRange.determineWeekForDay(19900731));
  }

  @Test
  public void testFirstDayOfLastYear() {
    String formattedDate = DateRange.firstDayOfLastYear();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19890101");
  }

  @Test
  public void testLastDayOfLastYear() {
    String formattedDate = DateRange.lastDayOfLastYear();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19891231");
  }

  @Test
  public void testFirstDayOfThisYear() {
    String formattedDate = DateRange.firstDayOfThisYear();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900101");
  }

  @Test
  public void testFirstDayOfLastQuarter() {
    String formattedDate = DateRange.firstDayOfLastQuarter();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900401");
  }

  @Test
  public void testLastDayOfLastQuarter() {
    String formattedDate = DateRange.lastDayOfLastQuarter();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900630");
  }

  @Test
  public void testFirstDayOfThisQuarter() {
    String formattedDate = DateRange.firstDayOfThisQuarter();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900701");
  }

  @Test
  public void testFirstDayOfLastMonth() {
    String formattedDate = DateRange.firstDayOfLastMonth();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900601");
  }

  @Test
  public void testLastDayOfLastMonth() {
    String formattedDate = DateRange.lastDayOfLastMonth();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900630");
  }

  @Test
  public void testFirstDayOfThisMonth() {
    String formattedDate = DateRange.firstDayOfThisMonth();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900701");
  }

  @Test
  public void testFirstDayOfLastWeek() {
    String formattedDate = DateRange.firstDayOfLastWeek();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900625");
  }

  @Test
  public void testLastDayOfLastWeek() {
    String formattedDate = DateRange.lastDayOfLastWeek();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900701");
  }

  @Test
  public void testFirstDayOfThisWeek() {
    String formattedDate = DateRange.firstDayOfThisWeek();
    Assert.assertNotNull(formattedDate);
    Assert.assertFalse(formattedDate.isEmpty());
    Assert.assertEquals(formattedDate, "19900702");
  }

  @Test
  public void testGetFirstAndLastMonthOfQuarterReturnsExpectedMonths() {
    Calendar test = Calendar.getInstance();
    // FIRST QUARTER
    test.set(Calendar.MONTH, 1); // February
    DateRange.calendar = test;
    Assert.assertEquals(Calendar.JANUARY, DateRange.getFirstMonthOfQuarter(test));
    Assert.assertEquals(Calendar.MARCH, DateRange.getLastMonthOfQuarter(test));

    // SECOND QUARTER
    test.set(Calendar.MONTH, 3); // April
    DateRange.calendar = test;
    Assert.assertEquals(Calendar.APRIL, DateRange.getFirstMonthOfQuarter(test));
    Assert.assertEquals(Calendar.JUNE, DateRange.getLastMonthOfQuarter(test));

    // THIRD QUARTER
    test.set(Calendar.MONTH, 7); // August
    DateRange.calendar = test;
    Assert.assertEquals(Calendar.JULY, DateRange.getFirstMonthOfQuarter(test));
    Assert.assertEquals(Calendar.SEPTEMBER, DateRange.getLastMonthOfQuarter(test));

    // FOURTH QUARTER
    test.set(Calendar.MONTH, 10); // November
    DateRange.calendar = test;
    Assert.assertEquals(Calendar.OCTOBER, DateRange.getFirstMonthOfQuarter(test));
    Assert.assertEquals(Calendar.DECEMBER, DateRange.getLastMonthOfQuarter(test));
  }

  //@Ignore("sanity")
  @Test
  public void printAllDateRanges() {
    DateRange.calendar = null;
    String[] dateRange;

    System.out.println("Today\'s date formatted as yyyyMMdd is " + DateRange.todaysDate());
    System.out.println("Yesterday\'s date formatted as yyyyMMdd is " + DateRange.yesterdaysDate());

    dateRange = DateRange.rangeThisWeek();
    System.out.println("Date range for this week formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    dateRange = DateRange.rangeLastWeek();
    System.out.println("Date range for last week formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    dateRange = DateRange.rangeMonthToDate();
    System.out.println("Date range for this month formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    dateRange = DateRange.rangeLastMonth();
    System.out.println("Date range for last month formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    dateRange = DateRange.rangeQuarterToDate();
    System.out.println("Date range for this quarter formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    dateRange = DateRange.rangeLastQuarter();
    System.out.println("Date range for last quarter formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    dateRange = DateRange.rangeYearToDate();
    System.out.println("Date range for this year formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    dateRange = DateRange.rangeLastYear();
    System.out.println("Date range for last year formatted as yyyyMMdd is " + dateRange[0] + " - " + dateRange[1]);

    System.out.println("Date formatted as yyyyMMdd is " + DateRange.formattedDate(new Date()));
    System.out.println("Date formatted as yyyyMM is " + DateRange.formattedMonth(new Date()));
  }

  private Calendar getTestCalendar() {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, 1990);
    cal.set(Calendar.MONTH, 6);
    cal.set(Calendar.DAY_OF_MONTH, 4);
    return cal;
  }

  private void getTestMonthsList(List<Integer> months, int duration) {
    Calendar cal = Calendar.getInstance();
    do {
      months.add(Integer.valueOf(DateRange.MONTH_FORMAT.format(cal.getTime())));
      cal.add(Calendar.MONTH, -1);
      duration--;
    } while (duration >= 0);
  }
}
