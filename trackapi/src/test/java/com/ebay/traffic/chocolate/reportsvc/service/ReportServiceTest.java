package com.ebay.traffic.chocolate.reportsvc.service;

import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import com.ebay.traffic.chocolate.reportsvc.constant.Granularity;
import com.ebay.traffic.chocolate.reportsvc.constant.ReportType;
import com.ebay.traffic.chocolate.reportsvc.dao.DataType;
import com.ebay.traffic.chocolate.reportsvc.dao.ReportDao;
import com.ebay.traffic.chocolate.reportsvc.dao.ReportDo;
import com.ebay.traffic.chocolate.reportsvc.dao.Tuple;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRecordsPerDay;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRecordsPerMonth;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

public class ReportServiceTest {

  private static List<ReportDo> reportDoList = createTestReportDoList();
  private static List<ReportDo> badReportDoList = createBadTestReportDoList();

  private static final String TEST_ID = "PUBLISHER_8888";
  private static final String TEST_MONTH = "199007";

  @InjectMocks
  private ReportService reportService = new ReportServiceImpl();

  @Mock
  private ReportDao reportDao;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testGenerateReportForOneDayReturnsListWithOneElement() throws ParseException {
    ReportRequest request = createTestRequest(1);

    Mockito.when(reportDao.getAllDataForDate(Mockito.anyString(), Mockito.anyString())).thenReturn(reportDoList);

    ReportResponse response = reportService.generateReportForRequest(request);
    Assert.assertEquals(1, response.getReport().size());
    Assert.assertEquals("1990-07-01", response.getReport().get(0).getMonth());
  }

  @Test
  public void testGenerateReportForOneMonthAndDateRangeReturnsListWithOneElement() throws ParseException {
    ReportRequest request = createTestRequest(1);
    request.setEndDate(19900706);

    Mockito.when(reportDao.getAllDataForDateRange(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(reportDoList);

    ReportResponse response = reportService.generateReportForRequest(request);
    Assert.assertEquals(1, response.getReport().size());
    Assert.assertEquals("1990-07-01", response.getReport().get(0).getMonth());
  }

  @Test
  public void testGenerateReportForOneMonthAndWeeKGranularityReturnsListWithOneElement() throws ParseException {
    ReportRequest request = createTestRequest(1);
    request.setEndDate(19900730);
    request.setGranularity(Granularity.WEEK);

    Mockito.when(reportDao.getAllDataForDateRange(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(reportDoList);

    ReportResponse response = reportService.generateReportForRequest(request);
    Assert.assertEquals(1, response.getReport().size());

    ReportRecordsPerMonth record = response.getReport().get(0);
    Assert.assertEquals("1990-07-01", record.getMonth());
    Assert.assertNotNull(record.getRecordsForMonth());

    assertDailyRecordAreForRightDays(record.getRecordsForMonth());
  }

  @Test
  public void testGenerateReportForOneMonthAndMonthGranularityReturnsListWithOneElementAndEmptySublist() throws ParseException {
    ReportRequest request = createTestRequest(1);
    request.setEndDate(19900730);
    request.setGranularity(Granularity.MONTH);

    Mockito.when(reportDao.getAllDataForDateRange(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(reportDoList);

    ReportResponse response = reportService.generateReportForRequest(request);
    Assert.assertEquals(1, response.getReport().size());

    ReportRecordsPerMonth record = response.getReport().get(0);
    Assert.assertEquals("1990-07-01", record.getMonth());
    Assert.assertNotNull(record.getRecordsForMonth());
    Assert.assertTrue(record.getRecordsForMonth().isEmpty());
  }

  @Test
  public void testGenerateReportForTwoMonthsReturnsListWithTwoElements() throws ParseException {
    ReportRequest request = createTestRequest(2);

    Mockito.when(reportDao.getAllDataGreaterThanDate(Mockito.anyString(), Mockito.anyString())).thenReturn(reportDoList);
    Mockito.when(reportDao.getAllDataLessThanDate(Mockito.anyString(), Mockito.anyString())).thenReturn(reportDoList);

    ReportResponse response = reportService.generateReportForRequest(request);
    Assert.assertEquals(2, response.getReport().size());
    Assert.assertEquals("1990-07-01", response.getReport().get(0).getMonth());
    Assert.assertEquals("1990-08-01", response.getReport().get(1).getMonth());
  }

  @Test
  public void testGenerateReportForMoreThanTwoMonthsReturnsListWithCorrectElements() throws ParseException {
    ReportRequest request = createTestRequest(4);

    Mockito.when(reportDao.getAllDataGreaterThanDate(TEST_ID, String.valueOf(request.getStartDate()))).thenReturn(reportDoList);
    Mockito.when(reportDao.getAllDataForMonth(TEST_ID, String.valueOf(request.getMonths().get(1)))).thenReturn(reportDoList);
    Mockito.when(reportDao.getAllDataForMonth(TEST_ID, String.valueOf(request.getMonths().get(2)))).thenReturn(reportDoList);
    Mockito.when(reportDao.getAllDataLessThanDate(TEST_ID, String.valueOf(request.getEndDate()))).thenReturn(reportDoList);

    ReportResponse response = reportService.generateReportForRequest(request);
    Assert.assertEquals(4, response.getReport().size());
    Assert.assertEquals("1990-07-01", response.getReport().get(0).getMonth());
    Assert.assertEquals("1990-08-01", response.getReport().get(1).getMonth());
    Assert.assertEquals("1990-09-01", response.getReport().get(2).getMonth());
    Assert.assertEquals("1990-10-01", response.getReport().get(3).getMonth());
  }

  private static void assertDailyRecordAreForRightDays(List<ReportRecordsPerDay> records) {
    Assert.assertEquals(6, records.size());
    Assert.assertEquals("1990-06-25", records.get(0).getDate());
    Assert.assertEquals("1990-07-02", records.get(1).getDate());
    Assert.assertEquals("1990-07-09", records.get(2).getDate());
    Assert.assertEquals("1990-07-16", records.get(3).getDate());
    Assert.assertEquals("1990-07-23", records.get(4).getDate());
    Assert.assertEquals("1990-07-30", records.get(5).getDate());
  }

  private static List<ReportDo> createTestReportDoList() {
    List<ReportDo> reportDoList = new ArrayList<>();

    String key1 = "PUBLISHER_8888_1990-07-04_CLICK_DESKTOP_FILTERED";
    String key2 = "PUBLISHER_8888_1990-07-04_IMPRESSION_DESKTOP_FILTERED";
    String key3 = "PUBLISHER_8888_1990-07-04_IMPRESSION_DESKTOP_RAW"; // Gross
    String key4 = "PUBLISHER_8888_1990-07-04_VIEWABLE_DESKTOP_FILTERED";

    Tuple<Long, Long> clickTuple = new Tuple<Long, Long>(200L, 647074800000L);
    Tuple<Long, Long> impressionTuple = new Tuple<Long, Long>(1000L, 647074800000L);
    Tuple<Long, Long> grossTuple = new Tuple<Long, Long>(2000L, 647074800000L);
    Tuple<Long, Long> viewableTuple = new Tuple<Long, Long>(500L, 647074800000L);

    ReportDo reportDo1 = new ReportDo(key1, Arrays.asList(clickTuple), DataType.CLICK, 19900704);
    ReportDo reportDo2 = new ReportDo(key2, Arrays.asList(impressionTuple), DataType.IMPRESSION, 19900704);
    ReportDo reportDo3 = new ReportDo(key3, Arrays.asList(grossTuple), DataType.GROSS_IMPRESSION, 19900704);
    ReportDo reportDo4 = new ReportDo(key4, Arrays.asList(viewableTuple), DataType.VIEWABLE, 19900704);

    reportDoList.add(reportDo1);
    reportDoList.add(reportDo2);
    reportDoList.add(reportDo3);
    reportDoList.add(reportDo4);

    return reportDoList;
  }

  private static List<ReportDo> createBadTestReportDoList() {
    List<ReportDo> reportDoList = new ArrayList<>();

    String key1 = "PUBLISHER_8888_1990-07-04_CLICK_DESKTOP_FILTERED";
    String key2 = "PUBLISHER_8888_1990-07-04_IMPRESSION_DESKTOP_FILTERED";
    String key3 = "PUBLISHER_8888_1990-07-04_IMPRESSION_DESKTOP_RAW"; // Gross
    String key4 = "PUBLISHER_8888_1990-07-04_VIEWABLE_DESKTOP_FILTERED";

    Tuple<Long, Long> clickTuple = new Tuple<Long, Long>(200L, 647074800000L);
    Tuple<Long, Long> impressionTuple = new Tuple<Long, Long>(1000L, 647074800000L);
    Tuple<Long, Long> grossTuple = new Tuple<Long, Long>(2000L, 647074800000L);
    Tuple<Long, Long> viewableTuple = new Tuple<Long, Long>(500L, 647074800000L);

    ReportDo reportDo1 = new ReportDo(key1, Arrays.asList(clickTuple), DataType.CLICK, 19900744);
    ReportDo reportDo2 = new ReportDo(key2, Arrays.asList(impressionTuple), DataType.IMPRESSION, 19900744);
    ReportDo reportDo3 = new ReportDo(key3, Arrays.asList(grossTuple), DataType.GROSS_IMPRESSION, 19900744);
    ReportDo reportDo4 = new ReportDo(key4, Arrays.asList(viewableTuple), DataType.VIEWABLE, 19900744);

    reportDoList.add(reportDo1);
    reportDoList.add(reportDo2);
    reportDoList.add(reportDo3);
    reportDoList.add(reportDo4);

    return reportDoList;
  }

  private static long randomTS() {
    long baseTimestamp = 1501545600000L; // UTC 2017/08/01 00:00:00
    long maxTimestamp = 1501631999000L; // UTC 2017/08/01 23:59:59

    return ThreadLocalRandom.current().nextLong(baseTimestamp, maxTimestamp);
  }

  private static ReportRequest createTestRequest(int months) {
    ReportRequest request = new ReportRequest();
    request.setId(TEST_ID);

    if (months == 1) {
      request.setMonths(Arrays.asList(199007));
      request.setEndDate(19900704);
    } else if (months == 2) {
      request.setMonths(Arrays.asList(199007, 199008));
      request.setEndDate(19900809);
    } else if (months == 4) {
      request.setMonths(Arrays.asList(199007, 199008, 199009, 199010));
      request.setEndDate(19901024);
    }

    request.setDateRange(DateRange.CUSTOM);
    request.setGranularity(Granularity.FINE);
    request.setStartDate(19900704);
    request.setReportType(ReportType.PARTNER);

    return request;
  }
}
