package com.ebay.traffic.chocolate.reportsvc.constant;

import java.text.ParseException;
import java.util.Calendar;

import org.joda.time.DateTime;
import org.joda.time.Days;

public enum Granularity {
  MONTH,
  WEEK,
  DAY,
  FINE,
  CUSTOM;

  /** Helper methods */

  public static Granularity getGranularityByName(String name) {
    for (Granularity granularity : Granularity.values()) {
      if (name.equalsIgnoreCase(granularity.name())) {
        return granularity;
      }
    }
    return DAY;
  }

  /**
   * Determine granularity at which to aggregate data for the given interval.
   *
   * @param numberOfDays number of days
   * @return Granularity
   */
  public static Granularity getGranularityForInterval(int numberOfDays) {
    if (numberOfDays == 0 || numberOfDays == 1) {
      return FINE;
    } else if (2 <= numberOfDays && numberOfDays <= 14) {
      return DAY;
    } else if (15 <= numberOfDays && numberOfDays <= 90) {
      return WEEK;
    } else {
      return MONTH;
    }
  }

  /**
   * Calculate the Granularity at which to group data, based on a date range.
   * @param startDate Start date for range
   * @param endDate End date for range
   * @return Granularity
   * @throws ParseException
   */
  public static Granularity getGranularityForCustomDateRange(String startDate, String endDate) throws ParseException {
    if (startDate.equals(endDate)) {
      return Granularity.FINE;
    }

    Calendar calStart = Calendar.getInstance();
    calStart.setTime(DateRange.DATE_FORMAT.parse(startDate));

    Calendar calEnd = Calendar.getInstance();
    calEnd.setTime(DateRange.DATE_FORMAT.parse(endDate));

    // Swap dates if start date is after end date, instead of wasting a request.
    if (calEnd.before(calStart)) {
      Calendar temp = calStart;
      calStart = calEnd;
      calEnd = temp;
    }

    int numberOfDays = Days.daysBetween(new DateTime(calStart), new DateTime(calEnd)).getDays() + 1;
    return getGranularityForInterval(numberOfDays);
  }

}
