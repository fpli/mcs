package com.ebay.traffic.chocolate.reportsvc.entity;

import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import com.ebay.traffic.chocolate.reportsvc.constant.ErrorType;
import com.ebay.traffic.chocolate.reportsvc.constant.Granularity;
import com.ebay.traffic.chocolate.reportsvc.constant.ReportType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ReportRequestTest {
  private static final String TEST_CAMPAIGN_ID = "1234";
  private static final String TEST_PARTNER_ID = "9999";
  private static final long LONG_CAMPAIGN_ID = 1234L;
  private static final long LONG_PARTNER_ID = 9999L;
  private static final String TEST_END_DATE = "2017-06-20";
  private static final String TEST_START_DATE = "2017-06-19";

  private static Map<String, String> createTestRequest(String partnerId, String campaignId, String dateRange) {
    Map<String, String> incomingRequest = new HashMap<>();
    incomingRequest.put("partnerId", partnerId);
    incomingRequest.put("campaignId", campaignId);
    incomingRequest.put("dateRange", dateRange);
    return incomingRequest;
  }

  private static Map<String, String> createTestRequest(String partnerId, String campaignId, String dateRange, String startDate, String endDate) {
    Map<String, String> incomingRequest = new HashMap<>();
    incomingRequest.put("partnerId", partnerId);
    incomingRequest.put("campaignId", campaignId);
    incomingRequest.put("dateRange", dateRange);
    incomingRequest.put("startDate", startDate);
    incomingRequest.put("endDate", endDate);
    return incomingRequest;
  }

  private static void assertExpectedValuesAreSet(ReportRequest request,
                                                 DateRange expectedDateRange,
                                                 Granularity expectedGranularity,
                                                 int expectedStartDate,
                                                 int expectedEndDate) {
    Assert.assertNotNull(request.getDateRange());
    Assert.assertNotNull(request.getGranularity());
    Assert.assertEquals(expectedDateRange, request.getDateRange());
    Assert.assertEquals(expectedGranularity, request.getGranularity());
    Assert.assertNotEquals(request.getStartDate(), 0);
    Assert.assertNotEquals(request.getEndDate(), 0);
    Assert.assertEquals(request.getStartDate(), expectedStartDate);
    Assert.assertEquals(request.getEndDate(), expectedEndDate);
  }

  @Test
  public void testCreateRequestWithNullPartnerAndNullCampaignShouldThrowException() {
    try {
      ReportRequest request = new ReportRequest(
              createTestRequest(null, null, DateRange.MONTH_TO_DATE.getParamName()));
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), ErrorType.BAD_PARTNER_INFO.getErrorKey());
    }
  }

  @Test
  public void testCreateRequestWithValidPartnerNullCampaignShouldSetPartnerReport() {
    ReportRequest request = null;

    try {
      request = new ReportRequest(
              createTestRequest(TEST_PARTNER_ID, null, DateRange.MONTH_TO_DATE.getParamName()));
    } catch (Exception e) {
      Assert.fail("Unexpected error");
    }

    Assert.assertNotNull(request);
    Assert.assertEquals(request.getId(), LONG_PARTNER_ID);
    Assert.assertNotNull(request.getReportType());
    Assert.assertEquals(request.getReportType(), ReportType.PARTNER);
  }

  @Test
  public void testCreateRequestWithValidPartnerValidCampaignShouldSetCampaignReport() {
    ReportRequest request = null;

    try {
      request = new ReportRequest(
              createTestRequest(TEST_PARTNER_ID, TEST_CAMPAIGN_ID, DateRange.MONTH_TO_DATE.getParamName()));
    } catch (Exception e) {
      Assert.fail("Unexpected error");
    }

    Assert.assertNotNull(request);
    Assert.assertEquals(request.getId(), LONG_CAMPAIGN_ID);
    Assert.assertNotNull(request.getReportType());
    Assert.assertEquals(request.getReportType(), ReportType.CAMPAIGN);
  }

  @Test
  public void testCreateRequestWithCustomDateRangeShouldThrowExceptionIfNoStartDate() {
    try {
      ReportRequest request = new ReportRequest(
              createTestRequest(TEST_PARTNER_ID, TEST_CAMPAIGN_ID, DateRange.CUSTOM.getParamName(), null, TEST_END_DATE));
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), ErrorType.BAD_START_END_DATE.getErrorKey());
    }
  }

  @Test
  public void testCreateRequestWithCustomDateRangeShouldThrowExceptionIfNoEndDate() {
    try {
      ReportRequest request = new ReportRequest(
              createTestRequest(TEST_PARTNER_ID, TEST_CAMPAIGN_ID, DateRange.CUSTOM.getParamName(), TEST_START_DATE, null));
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), ErrorType.BAD_START_END_DATE.getErrorKey());
    }
  }

  @Test
  public void testCreateRequestWithCustomDateRangeShouldThrowExceptionIfBadStartDateFormat() {
    try {
      ReportRequest request = new ReportRequest(
              createTestRequest(TEST_PARTNER_ID, TEST_CAMPAIGN_ID, DateRange.CUSTOM.getParamName(), "2018/07/11", TEST_END_DATE));
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), ErrorType.BAD_START_END_DATE.getErrorKey());
    }
  }

  @Test
  public void testCreateRequestWithCustomDateRangeShouldThrowExceptionIfBadEndDateFormat() {
    try {
      ReportRequest request = new ReportRequest(
              createTestRequest(TEST_PARTNER_ID, TEST_CAMPAIGN_ID, DateRange.CUSTOM.getParamName(), TEST_START_DATE, "2018/07/11"));
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), ErrorType.BAD_START_END_DATE.getErrorKey());
    }
  }

  @Test
  public void testCreateRequestWithCustomDateRangeValidDataShouldSetFormattedStartAndEndDates() {
    ReportRequest request = null;
    try {
      request = new ReportRequest(
              createTestRequest(TEST_PARTNER_ID, TEST_CAMPAIGN_ID, DateRange.CUSTOM.getParamName(), TEST_START_DATE, TEST_END_DATE));
    } catch (Exception e) {
      Assert.fail("Unexpected error with custom start and end dates.");
    }
    assertExpectedValuesAreSet(request, DateRange.CUSTOM, Granularity.DAY, 20170619, 20170620);
  }
}
