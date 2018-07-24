package com.ebay.traffic.chocolate.reportsvc;

import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import com.ebay.traffic.chocolate.reportsvc.constant.ErrorType;
import com.ebay.traffic.chocolate.reportsvc.constant.Granularity;
import com.ebay.traffic.chocolate.reportsvc.constant.ReportType;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRecord;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRecordsPerDay;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRecordsPerMonth;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import org.junit.Test;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class CommonTest {

  @Test
  public void testGetErrorTypeByErrorKeyReturnsRightEnum() {
    assertSame(ErrorType.INTERNAL_SERVICE_ERROR, ErrorType.getErrorTypeByErrorCode(ErrorType.INTERNAL_SERVICE_ERROR.getErrorKey()));
    assertSame(ErrorType.BAD_PARTNER_INFO, ErrorType.getErrorTypeByErrorCode(ErrorType.BAD_PARTNER_INFO.getErrorKey()));
    assertSame(ErrorType.BAD_CAMPAIGN_INFO,
            ErrorType.getErrorTypeByErrorCode(ErrorType.BAD_CAMPAIGN_INFO.getErrorKey().toLowerCase()));
    assertSame(ErrorType.BAD_START_END_DATE, ErrorType.getErrorTypeByErrorCode("badStartenddate"));
    assertSame(ErrorType.INTERNAL_SERVICE_ERROR, ErrorType.getErrorTypeByErrorCode("Bad Key"));
  }

  @Test
  public void testGetReportTypeByValueReturnsRightEnum() {
    assertSame(ReportType.PARTNER, ReportType.getByReportTypeValue(ReportType.PARTNER.name()));
    assertSame(ReportType.CAMPAIGN, ReportType.getByReportTypeValue(ReportType.CAMPAIGN.name().toLowerCase()));
    assertSame(ReportType.NONE, ReportType.getByReportTypeValue("Invalid"));
  }

  @Test
  public void testGetGranularityByBameReturnsRightEnum() {
    assertSame(Granularity.MONTH, Granularity.getGranularityByName(Granularity.MONTH.name()));
    assertSame(Granularity.WEEK, Granularity.getGranularityByName(Granularity.WEEK.name().toLowerCase()));
    assertSame(Granularity.DAY, Granularity.getGranularityByName("Day"));
    assertSame(Granularity.FINE, Granularity.getGranularityByName(Granularity.FINE.name()));
    assertSame(Granularity.CUSTOM, Granularity.getGranularityByName("Custom"));
    assertSame(Granularity.DAY, Granularity.getGranularityByName("Custome"));
  }

  @Test
  public void testGetDateRangeForParamNameReturnsRightEnum() {
    assertSame(DateRange.LAST_YEAR, DateRange.getDateRangeForParamName(DateRange.LAST_YEAR.getParamName()));
    assertSame(DateRange.YEAR_TO_DATE, DateRange.getDateRangeForParamName(DateRange.YEAR_TO_DATE.getParamName()));
    assertSame(DateRange.LAST_QUARTER, DateRange.getDateRangeForParamName(DateRange.LAST_QUARTER.getParamName()));
    assertSame(DateRange.QUARTER_TO_DATE, DateRange.getDateRangeForParamName(DateRange.QUARTER_TO_DATE.getParamName()));
    assertSame(DateRange.LAST_MONTH, DateRange.getDateRangeForParamName(DateRange.LAST_MONTH.getParamName()));
    assertSame(DateRange.MONTH_TO_DATE, DateRange.getDateRangeForParamName(DateRange.MONTH_TO_DATE.getParamName()));
    assertSame(DateRange.LAST_WEEK, DateRange.getDateRangeForParamName(DateRange.LAST_WEEK.getParamName()));
    assertSame(DateRange.WEEK_TO_DATE, DateRange.getDateRangeForParamName(DateRange.WEEK_TO_DATE.getParamName()));
    assertSame(DateRange.YESTERDAY, DateRange.getDateRangeForParamName(DateRange.YESTERDAY.getParamName()));
    assertSame(DateRange.TODAY, DateRange.getDateRangeForParamName(DateRange.TODAY.getParamName()));
    assertSame(DateRange.CUSTOM, DateRange.getDateRangeForParamName(DateRange.CUSTOM.getParamName()));
    assertSame(DateRange.MONTH_TO_DATE, DateRange.getDateRangeForParamName("Invalid"));
  }

  @Test
  public void testEqualsForEqualObjectsReturnsTrue() throws Exception {
    ReportRecord reportRecord = new ReportRecord(123456L, 50, 51, 100, 50, 150, 151, 10, 11, 20, 21);
    ReportRecord anotherRecord = new ReportRecord();
    anotherRecord.setTimestamp(123456L);
    anotherRecord.setClickCount(50);
    anotherRecord.setGrossClickCount(51);
    anotherRecord.setImpressionCount(100);
    anotherRecord.setGrossImpressionCount(50);
    anotherRecord.setViewableImpressionCount(150);
    anotherRecord.setGrossViewableImpressionCount(151);
    anotherRecord.setMobileClickCount(10);
    anotherRecord.setGrossMobileClickCount(11);
    anotherRecord.setMobileImpressionCount(20);
    anotherRecord.setGrossMobileImpressionCount(21);

    assertTrue(reportRecord.equals(anotherRecord));

    ReportRecordsPerDay recordForDay = new ReportRecordsPerDay();
    recordForDay.setDate("19900704");
    ReportRecordsPerDay anotherRecordForDay = new ReportRecordsPerDay();
    anotherRecordForDay.setDate("19900704");

    assertTrue(recordForDay.equals(anotherRecordForDay));
    assertFalse(reportRecord.equals(recordForDay));

    ReportRecordsPerMonth recordForMonth = new ReportRecordsPerMonth("199007");
    ReportRecordsPerMonth anotherRecordForMonth = new ReportRecordsPerMonth();
    anotherRecordForMonth.setMonth("199007");

    assertTrue(recordForMonth.equals(anotherRecordForMonth));
    assertFalse(recordForDay.equals(recordForMonth));
    assertFalse(recordForMonth.equals(recordForDay));

    Map<String, String> testRequest = new HashMap<String, String>() {
      private static final long serialVersionUID = 1L;

      {
        put("partnerId", "8888");
        put("campaignId", "1234");
        put("dateRange", DateRange.CUSTOM.getParamName());
        put("startDate", "2000-05-02");
        put("endDate", "2000-05-03");
      }
    };
    ReportRequest request = new ReportRequest(testRequest);
    ReportRequest anotherRequest = new ReportRequest(testRequest);

    assertTrue(request.equals(anotherRequest));
  }

  @Test
  public void testEqualsForUnequalsRequestReturnsFalse() throws Exception {
    Map<String, String> testRequest = new HashMap<String, String>() {
      private static final long serialVersionUID = 1L;

      {
        put("partnerId", "8888");
        put("campaignId", null);
        put("dateRange", DateRange.CUSTOM.getParamName());
        put("startDate", "2000-05-02");
        put("endDate", "2000-05-03");
      }
    };
    ReportRequest request = new ReportRequest(testRequest);

    testRequest.put("campaignId", "9999");
    ReportRequest anotherRequest = new ReportRequest(testRequest);
    assertFalse(request.equals(anotherRequest));

    testRequest.put("campaignId", null);
    testRequest.put("dateRange", DateRange.MONTH_TO_DATE.getParamName());
    anotherRequest = new ReportRequest(testRequest);
    assertFalse(request.equals(anotherRequest));

    testRequest.put("dateRange", DateRange.CUSTOM.getParamName());
    testRequest.put("startDate", "2000-05-03");
    anotherRequest = new ReportRequest(testRequest);
    assertFalse(request.equals(anotherRequest));

    testRequest.put("startDate", "2000-05-02");
    testRequest.put("endDate", "2000-05-02");
    anotherRequest = new ReportRequest(testRequest);
    assertFalse(request.equals(anotherRequest));

    ReportRecord record = new ReportRecord();
    assertFalse(request.equals(record));
  }

  @Test
  public void testEqualsForUnequalReportRecordsReturnsFalse() throws ParseException {
    ReportRecord reportRecord = new ReportRecord(123456L, 50, 51, 100, 101, 150, 151, 20, 21, 40, 41);
    ReportRecord unequalRecord = new ReportRecord(123457L, 50, 51, 100, 101, 150, 151, 20, 21, 40, 41);
    assertFalse(reportRecord.equals(unequalRecord));

    unequalRecord = new ReportRecord(123456L, 100, 101, 100, 101, 150, 151, 20, 21, 40, 41);
    assertFalse(reportRecord.equals(unequalRecord));

    unequalRecord = new ReportRecord(123456L, 50, 51, 150, 151, 150, 151, 20, 21, 40, 41);
    assertFalse(reportRecord.equals(unequalRecord));

    unequalRecord = new ReportRecord(123456L, 50, 51, 100, 101, 50, 51, 20, 21, 40, 41);
    assertFalse(reportRecord.equals(unequalRecord));

    unequalRecord = new ReportRecord(123456L, 50, 51, 100, 101, 150, 151, 150, 151, 40, 41);
    assertFalse(reportRecord.equals(unequalRecord));
  }

  @Test
  public void testEqualsForUnequalRecordsPerDayReturnsFalse() throws ParseException {
    ReportRecordsPerDay recordForDay = new ReportRecordsPerDay("19900704", Granularity.DAY);
    ReportRecordsPerDay unequalRecordForDay = new ReportRecordsPerDay("20000520", Granularity.DAY);
    assertFalse(recordForDay.equals(unequalRecordForDay));

    unequalRecordForDay = new ReportRecordsPerDay("19900704", Granularity.DAY);
    unequalRecordForDay.setAggregatedClickCount(10);
    assertFalse(recordForDay.equals(unequalRecordForDay));

    unequalRecordForDay.setAggregatedClickCount(0);
    unequalRecordForDay.setAggregatedImpressionCount(10);
    assertFalse(recordForDay.equals(unequalRecordForDay));

    unequalRecordForDay.setAggregatedImpressionCount(0);
    unequalRecordForDay.setAggregatedGrossImpressionCount(10);
    assertFalse(recordForDay.equals(unequalRecordForDay));

    unequalRecordForDay.setAggregatedGrossImpressionCount(0);
    unequalRecordForDay.setAggregatedViewableImpressionCount(10);
    assertFalse(recordForDay.equals(unequalRecordForDay));
  }

  @Test
  public void testEqualsForUnequalRecordsPerMonthReturnsFalse() {
    ReportRecordsPerMonth recordForMonth = new ReportRecordsPerMonth("199007");
    ReportRecordsPerMonth unequalRecordForMonth = new ReportRecordsPerMonth("200005");

    assertFalse(recordForMonth.equals(unequalRecordForMonth));
    unequalRecordForMonth = new ReportRecordsPerMonth("199007");
    unequalRecordForMonth.setAggregatedClickCount(10);
    assertFalse(recordForMonth.equals(unequalRecordForMonth));

    unequalRecordForMonth.setAggregatedClickCount(0);
    unequalRecordForMonth.setAggregatedImpressionCount(10);
    assertFalse(recordForMonth.equals(unequalRecordForMonth));

    unequalRecordForMonth.setAggregatedImpressionCount(0);
    unequalRecordForMonth.setAggregatedGrossImpressionCount(10);
    assertFalse(recordForMonth.equals(unequalRecordForMonth));

    unequalRecordForMonth.setAggregatedGrossImpressionCount(0);
    unequalRecordForMonth.setAggregatedViewableImpressionCount(10);
    assertFalse(recordForMonth.equals(unequalRecordForMonth));
  }

  @Test
  public void testAllToStringReturnExpectedStrings() throws ParseException {
    ReportRequest request = new ReportRequest();
    request.setId("PUBLISHER_8888");
    request.setReportType(ReportType.PARTNER);
    request.setDateRange(DateRange.YEAR_TO_DATE);
    request.setStartDate(20170101);
    request.setEndDate(20170801);

    String expectedString = "Request [keyPrefix: PUBLISHER_8888, reportType: PARTNER, dateRange: YEAR_TO_DATE, granularity: MONTH, "
            + "startDate: 20170101, endDate: 20170801]";
    assertEquals(expectedString, request.toString());

    request.setReportType(null);
    request.setDateRange(null);
    request.setGranularity(null);
    request.setStartDate(20170801);

    expectedString = "Request [keyPrefix: PUBLISHER_8888, reportType: NONE, dateRange: MONTH_TO_DATE, granularity: DAY, "
            + "startDate: 20170801, endDate: 20170801]";
    assertEquals(expectedString, request.toString());

    ReportRecord record = new ReportRecord(1234L, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    expectedString = "ReportRecord [timestamp: 1234, clicks: 1, gross clicks: 2, "
            + "impressions: 3, gross impressions: 4, "
            + "viewable impressions: 5, gross viewable impressions: 6, "
            + "mobile clicks: 7, gross mobile clicks: 8, "
            + "mobile impressions: 9, gross mobile impressions: 10]";
    assertEquals(expectedString, record.toString());

    ReportRecordsPerDay recordPerDay = new ReportRecordsPerDay("1990-07-04", Granularity.WEEK);
    recordPerDay.setAggregatedClickCount(10);
    recordPerDay.setAggregatedGrossClickCount(11);
    recordPerDay.setAggregatedImpressionCount(20);
    recordPerDay.setAggregatedGrossImpressionCount(30);
    recordPerDay.setAggregatedViewableImpressionCount(10);
    recordPerDay.setAggregatedGrossViewableImpressionCount(11);
    expectedString = "ReportRecordsPerDay [day: 1990-07-04, aggrClicks: 10, aggrGrossClicks: 11, "
            + "aggrImpressions: 20, aggrGrossImpressions: 30, "
            + "aggrViewableImpressions: 10, aggrGrossViewableImpressions: 11, "
            + "aggrMobileClicks: 0, aggrGrossMobileClicks: 0, "
            + "aggrMobileImpressions: 0, aggrGrossMobileImpressions: 0]";
    assertEquals(expectedString, recordPerDay.toString());

    ReportRecordsPerMonth recordPerMonth = new ReportRecordsPerMonth("19900701");
    recordPerMonth.setAggregatedClickCount(10);
    recordPerMonth.setAggregatedGrossClickCount(11);
    recordPerMonth.setAggregatedImpressionCount(20);
    recordPerMonth.setAggregatedGrossImpressionCount(30);
    recordPerMonth.setAggregatedViewableImpressionCount(10);
    recordPerMonth.setAggregatedGrossViewableImpressionCount(11);
    expectedString = "ReportRecordsPerMonth [month: 19900701, aggrClicks: 10, aggrGrossClicks: 11, "
            + "aggrImpressions: 20, aggrGrossImpressions: 30, "
            + "aggrViewableImpressions: 10, aggrGrossViewableImpressions: 11, "
            + "aggrMobileClicks: 0, aggrGrossMobileClicks: 0, "
            + "aggrMobileImpressions: 0, aggrGrossMobileImpressions: 0]";
    assertEquals(expectedString, recordPerMonth.toString());
  }
}
