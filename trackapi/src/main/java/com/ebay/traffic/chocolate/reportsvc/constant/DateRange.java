package com.ebay.traffic.chocolate.reportsvc.constant;

import org.joda.time.DateTime;
import org.joda.time.Months;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public enum DateRange {
  // Entire last year, in month granularity
  LAST_YEAR("lastyear", Granularity.MONTH),
  // Start from first day of this year to current date, in month granularity
  YEAR_TO_DATE("yeartodate", Granularity.MONTH),
  // Entire last quarter, in week granularity
  LAST_QUARTER("lastquarter", Granularity.WEEK),
  // Start from first day of this quarter to current date, in week granularity
  QUARTER_TO_DATE("quartertodate", Granularity.WEEK),
  // Entire last month, in day granularity
  LAST_MONTH("lastmonth", Granularity.DAY),
  // Start from first day of this month to current date, in day granularity
  MONTH_TO_DATE("monthtodate", Granularity.DAY),
  // Entire last week, in day granularity
  LAST_WEEK("lastweek", Granularity.DAY),
  // Start from first day of this week to current date, in day granularity
  WEEK_TO_DATE("weektodate", Granularity.DAY),
  YESTERDAY("yesterday", Granularity.FINE),
  TODAY("today", Granularity.FINE),
  CUSTOM("custom", Granularity.CUSTOM);

  private String paramName;
  private Granularity granularity;

  public static final TimeZone TIMEZONE = TimeZone.getTimeZone("UTC");

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
  public static final SimpleDateFormat MONTH_FORMAT = new SimpleDateFormat("yyyyMM");
  public static final SimpleDateFormat REQUEST_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  public static Calendar calendar;

  static {
    DATE_FORMAT.setLenient(false);
    DATE_FORMAT.setTimeZone(TIMEZONE);
    MONTH_FORMAT.setLenient(false);
    MONTH_FORMAT.setTimeZone(TIMEZONE);
    REQUEST_DATE_FORMAT.setLenient(false);
    REQUEST_DATE_FORMAT.setTimeZone(TIMEZONE);
    calendar = null;
  }

  DateRange(String paramName, Granularity granularity) {
    this.paramName = paramName;
    this.granularity = granularity;
  }

  public String getParamName() {
    return paramName;
  }

  public Granularity getGranularity() {
    return granularity;
  }

  /**
   * Helper methods
   */

  public static DateRange getDateRangeForParamName(String paramName) {
    switch (paramName) {
      case ("lastyear"):
        return LAST_YEAR;
      case ("yeartodate"):
        return YEAR_TO_DATE;
      case ("lastquarter"):
        return LAST_QUARTER;
      case ("quartertodate"):
        return QUARTER_TO_DATE;
      case ("lastmonth"):
        return LAST_MONTH;
      case ("lastweek"):
        return LAST_WEEK;
      case ("weektodate"):
        return WEEK_TO_DATE;
      case ("yesterday"):
        return YESTERDAY;
      case ("today"):
        return TODAY;
      case ("custom"):
        return CUSTOM;
      case ("monthtodate"):
      default:
        return MONTH_TO_DATE;
    }
  }

  /**
   * Given start and end dates, return a list of all the months in between.
   *
   * @param startDate
   * @param endDate
   * @throws ParseException
   */
  public static List<Integer> getMonthsForDateRange(String startDate, String endDate) throws ParseException {
    List<Integer> months = new ArrayList<>();

    Calendar calStart = Calendar.getInstance(TIMEZONE);
    calStart.setTime(DATE_FORMAT.parse(startDate));

    if (startDate.equals(endDate)) {
      months.add(Integer.valueOf(MONTH_FORMAT.format(calStart.getTime())));
      return months;
    }

    Calendar calEnd = Calendar.getInstance(TIMEZONE);
    calEnd.setTime(DATE_FORMAT.parse(endDate));

    // Swap dates if start date is after end date, instead of wasting a request.
    if (calEnd.before(calStart)) {
      Calendar temp = calStart;
      calStart = calEnd;
      calEnd = temp;
    }

    DateTime dt1 = new DateTime(calStart);
    DateTime dt2 = new DateTime(calEnd);
    Months monthDiff = Months.monthsBetween(dt1, dt2);
    int diff = monthDiff.getMonths() + 1;

    for (int i = 1; i <= diff; i++) {
      months.add(Integer.valueOf(MONTH_FORMAT.format(calStart.getTime())));
      calStart.add(Calendar.MONTH, 1);
    }

    return months;
  }

  /**
   * Calculate the start date and end dates for the given date range.
   *
   * @param dateRange
   * @return String[]; 0 - start date, 1 - end date.
   */
  public static String[] getDates(DateRange dateRange) {
    switch (dateRange) {
      case LAST_YEAR:
        return rangeLastYear();
      case YEAR_TO_DATE:
        return rangeYearToDate();
      case LAST_QUARTER:
        return rangeLastQuarter();
      case QUARTER_TO_DATE:
        return rangeQuarterToDate();
      case LAST_MONTH:
        return rangeLastMonth();
      case LAST_WEEK:
        return rangeLastWeek();
      case WEEK_TO_DATE:
        return rangeThisWeek();
      case YESTERDAY:
        return new String[]{yesterdaysDate(), yesterdaysDate()};
      case TODAY:
        return new String[]{todaysDate(), todaysDate()};
      case CUSTOM:
        return new String[2];
      case MONTH_TO_DATE:
      default:
        return rangeMonthToDate();
    }
  }

  /**
   * Return calendar instance set at midnight of given date.
   *
   * @param date
   * @return Calendar
   * @throws ParseException
   */
  public static Calendar getMidnightForDay(String date) throws ParseException {
    Calendar thisDay = Calendar.getInstance(TIMEZONE);
    thisDay.setTime(REQUEST_DATE_FORMAT.parse(date));
    thisDay.set(Calendar.HOUR, 0);
    thisDay.set(Calendar.MINUTE, 0);
    thisDay.set(Calendar.SECOND, 0);
    return thisDay;
  }

  /** Date range utility methods */

  /**
   * Get today's date in yyyyMMdd format.
   *
   * @return String today's date.
   */
  public static String todaysDate() {
    Calendar today = getCalendarInstance();
    return formattedDate(today.getTime());
  }

  /**
   * Get yesterday's date in yyyyMMdd format.
   *
   * @return String yesterday's date.
   */
  public static String yesterdaysDate() {
    Calendar yesterday = getCalendarInstance();
    yesterday.add(Calendar.DAY_OF_MONTH, -1);
    return formattedDate(yesterday.getTime());
  }

  /**
   * Get the first and last day of this week, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of this week, 1 - today.
   */
  public static String[] rangeThisWeek() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfThisWeek();
    firstAndLast[1] = todaysDate();
    return firstAndLast;
  }

  /**
   * Get the first and last day of last week, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of last week, 1 - last day of last week.
   */
  public static String[] rangeLastWeek() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfLastWeek();
    firstAndLast[1] = lastDayOfLastWeek();
    return firstAndLast;
  }

  /**
   * Get the first and last day of this month, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of this month, 1 - today.
   */
  public static String[] rangeMonthToDate() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfThisMonth();
    firstAndLast[1] = todaysDate();
    return firstAndLast;
  }

  /**
   * Get the first and last day of last month, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of last month, 1 - last day of last month.
   */
  public static String[] rangeLastMonth() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfLastMonth();
    firstAndLast[1] = lastDayOfLastMonth();
    return firstAndLast;
  }

  /**
   * Get the first and last day of this quarter, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of this quarter, 1 - today.
   */
  public static String[] rangeQuarterToDate() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfThisQuarter();
    firstAndLast[1] = todaysDate();
    return firstAndLast;
  }

  /**
   * Get the first and last day of last quarter, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of last quarter, 1 - last day of last quarter.
   */
  public static String[] rangeLastQuarter() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfLastQuarter();
    firstAndLast[1] = lastDayOfLastQuarter();
    return firstAndLast;
  }

  /**
   * Get the first and last day of this year, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of this year, 1 - today.
   */
  public static String[] rangeYearToDate() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfThisYear();
    firstAndLast[1] = todaysDate();
    return firstAndLast;
  }

  /**
   * Get the first and last day of last year, in yyyyMMdd format.
   *
   * @return String[]; 0 - first day of last year, 1 - last day of last year.
   */
  public static String[] rangeLastYear() {
    String[] firstAndLast = new String[2];
    firstAndLast[0] = firstDayOfLastYear();
    firstAndLast[1] = lastDayOfLastYear();
    return firstAndLast;
  }

  public static String formattedDate(Date date) {
    return (date == null) ? null : DATE_FORMAT.format(date);
  }

  public static String formattedMonth(Date date) {
    return (date == null) ? null : MONTH_FORMAT.format(date);
  }

  public static String formatRequestDate(String date) throws ParseException {
    return (date == null) ? null : DATE_FORMAT.format(REQUEST_DATE_FORMAT.parse(date));
  }

  public static String convertDateToRequestFormat(String date) throws ParseException {
    return (date == null) ? "" : REQUEST_DATE_FORMAT.format(DATE_FORMAT.parse(date));
  }

  public static String convertMonthToRequestFormat(String month) throws ParseException {
    return (month == null) ? "" : REQUEST_DATE_FORMAT.format(MONTH_FORMAT.parse(month));
  }

  /**
   * Given a month, return the list of first days of the week, for all weeks in the month.
   *
   * @param month
   * @return String[] - list of first days of each week in month
   * @throws ParseException
   */
  public static String[] getWeeksForMonth(int month) throws ParseException {
    Date time = DateRange.MONTH_FORMAT.parse(String.valueOf(month));
    Date today = new Date();
    Calendar start = Calendar.getInstance();
    start.setFirstDayOfWeek(Calendar.MONDAY);
    start.setTime(time);
    int weeksInMonth = start.getActualMaximum(Calendar.WEEK_OF_MONTH);
    List<String> weeks = new ArrayList<String>();

    start.set(Calendar.DAY_OF_WEEK, start.getFirstDayOfWeek());

    int i = 0;
    do {
      weeks.add(DateRange.DATE_FORMAT.format(start.getTime()));
      start.add(Calendar.WEEK_OF_MONTH, 1);
    } while (++i < weeksInMonth && start.getTime().before(today));

    return weeks.toArray(new String[weeks.size()]);
  }

  /**
   * Given a day, determine the first day of its week.
   *
   * @param day
   * @return
   * @throws ParseException
   */
  public static int determineWeekForDay(int day) throws ParseException {
    Calendar thisDay = Calendar.getInstance();
    thisDay.setFirstDayOfWeek(Calendar.MONDAY);
    thisDay.setTime(DateRange.DATE_FORMAT.parse(String.valueOf(day)));
    thisDay.set(Calendar.DAY_OF_WEEK, thisDay.getFirstDayOfWeek());
    return Integer.valueOf(DateRange.DATE_FORMAT.format(thisDay.getTime()));
  }

  private static Calendar getLastWeek() {
    Calendar lastWeek = getCalendarInstance();
    lastWeek.add(Calendar.WEEK_OF_MONTH, -1);
    return lastWeek;
  }

  private static Calendar getLastMonth() {
    Calendar lastMonth = getCalendarInstance();
    lastMonth.add(Calendar.MONTH, -1);
    return lastMonth;
  }

  private static Calendar getLastYear() {
    Calendar lastYear = getCalendarInstance();
    lastYear.add(Calendar.YEAR, -1);
    return lastYear;
  }

  public static String firstDayOfLastYear() {
    Calendar firstDayLastYear = getLastYear();
    firstDayLastYear.set(Calendar.MONTH, firstDayLastYear.getActualMinimum(Calendar.MONTH));
    firstDayLastYear.set(Calendar.DAY_OF_MONTH, 1);
    return formattedDate(firstDayLastYear.getTime());
  }

  public static String lastDayOfLastYear() {
    Calendar lastDayLastYear = getLastYear();
    lastDayLastYear.set(Calendar.MONTH, lastDayLastYear.getActualMaximum(Calendar.MONTH));
    lastDayLastYear.set(Calendar.DAY_OF_MONTH, lastDayLastYear.getActualMaximum(Calendar.DAY_OF_MONTH));
    return formattedDate(lastDayLastYear.getTime());
  }

  public static String firstDayOfThisYear() {
    Calendar firstDayThisYear = getCalendarInstance();
    firstDayThisYear.set(Calendar.DAY_OF_YEAR, 1);
    return formattedDate(firstDayThisYear.getTime());
  }

  public static String firstDayOfLastQuarter() {
    Calendar firstDayLastQtr = getCalendarInstance();
    firstDayLastQtr.add(Calendar.MONTH, -3);
    firstDayLastQtr.set(Calendar.MONTH, getFirstMonthOfQuarter(firstDayLastQtr));
    firstDayLastQtr.getTime();
    firstDayLastQtr.set(Calendar.DAY_OF_MONTH, 1);
    return formattedDate(firstDayLastQtr.getTime());
  }

  public static String lastDayOfLastQuarter() {
    Calendar lastDayLastQtr = getCalendarInstance();
    lastDayLastQtr.add(Calendar.MONTH, -3);
    lastDayLastQtr.set(Calendar.MONTH, getLastMonthOfQuarter(lastDayLastQtr));
    lastDayLastQtr.set(Calendar.DAY_OF_MONTH, lastDayLastQtr.getActualMaximum(Calendar.DAY_OF_MONTH));
    return formattedDate(lastDayLastQtr.getTime());
  }

  public static String firstDayOfThisQuarter() {
    Calendar firstDayThisQtr = getCalendarInstance();
    firstDayThisQtr.set(Calendar.MONTH, getFirstMonthOfQuarter(firstDayThisQtr));
    firstDayThisQtr.set(Calendar.DAY_OF_MONTH, 1);
    return formattedDate(firstDayThisQtr.getTime());
  }

  public static String firstDayOfLastMonth() {
    Calendar firstDayLastMonth = getLastMonth();
    firstDayLastMonth.set(Calendar.DAY_OF_MONTH, 1);
    return formattedDate(firstDayLastMonth.getTime());
  }

  public static String lastDayOfLastMonth() {
    Calendar lastDayLastMonth = getLastMonth();
    lastDayLastMonth.set(Calendar.DAY_OF_MONTH, lastDayLastMonth.getActualMaximum(Calendar.DATE));
    return formattedDate(lastDayLastMonth.getTime());
  }

  public static String firstDayOfThisMonth() {
    Calendar firstDayThisMonth = getCalendarInstance();
    firstDayThisMonth.set(Calendar.DAY_OF_MONTH, 1);
    return formattedDate(firstDayThisMonth.getTime());
  }

  public static String firstDayOfLastWeek() {
    Calendar firstDayLastWeek = getLastWeek();
    firstDayLastWeek.set(Calendar.DAY_OF_WEEK, firstDayLastWeek.getFirstDayOfWeek());
    return formattedDate(firstDayLastWeek.getTime());
  }

  public static String lastDayOfLastWeek() {
    Calendar lastDayLastweek = getLastWeek();
    lastDayLastweek.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    return formattedDate(lastDayLastweek.getTime());
  }

  public static String firstDayOfThisWeek() {
    Calendar firstDayThisWeek = getCalendarInstance();
    firstDayThisWeek.set(Calendar.DAY_OF_WEEK, firstDayThisWeek.getFirstDayOfWeek());
    return formattedDate(firstDayThisWeek.getTime());
  }

  static int getFirstMonthOfQuarter(Calendar cal) {
    int quarter = (cal.get(Calendar.MONTH) / 3);

    switch (quarter) {
      case 0:
        return Calendar.JANUARY;
      case 1:
        return Calendar.APRIL;
      case 2:
        return Calendar.JULY;
      case 3:
      default:
        return Calendar.OCTOBER;
    }
  }

  static int getLastMonthOfQuarter(Calendar cal) {
    int quarter = (cal.get(Calendar.MONTH) / 3);

    switch (quarter) {
      case 0:
        return Calendar.MARCH;
      case 1:
        return Calendar.JUNE;
      case 2:
        return Calendar.SEPTEMBER;
      case 3:
      default:
        return Calendar.DECEMBER;
    }
  }

  private static Calendar getCalendarInstance() {
    Calendar cal;
    if (DateRange.calendar != null) {
      cal = (Calendar) DateRange.calendar.clone();
    } else {
      cal = Calendar.getInstance(TIMEZONE);
    }
    cal.setFirstDayOfWeek(Calendar.MONDAY);
    return cal;
  }

  /* for testing purposes only */
  public static void updateTimezone(TimeZone timezone) {
    DATE_FORMAT.setTimeZone(timezone);
    MONTH_FORMAT.setTimeZone(timezone);
    REQUEST_DATE_FORMAT.setTimeZone(timezone);
  }
}
