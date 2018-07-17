package com.ebay.traffic.chocolate.reportsvc.dao;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.ebay.traffic.chocolate.mkttracksvc.dao.CouchbaseClientMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReportDaoTest {

  @BeforeClass
  public static void construct() throws Exception {
    CouchbaseClientMock.createClient();
    prepareTestData(CouchbaseClientMock.getBucket());
  }

  @AfterClass
  public static void tearDown() {
    CouchbaseClientMock.getBucket().close();
    CouchbaseClientMock.tearDown();
  }

  @Test
  public void testGetAllDataForMonthWithExistingMonthShouldReturnData() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataForMonth("PUBLISHER_11", "201801");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(12, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_MOBILE_FILTERED"));

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_CLICK_MOBILE_FILTERED"));
  }

  @Test
  public void testGetAllDataForMonthWithNonExistingMonthShouldReturnEmptyList() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataForMonth("PUBLISHER_11", "201701");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(0, list.size());
  }

  @Test
  public void testGetAllDataForDateWithExistingDateShouldReturnData() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataForDate("PUBLISHER_11", "20180101");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(6, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_MOBILE_FILTERED"));
  }

  @Test
  public void testGetAllDataForDateWithNonExistingDateShouldReturnEmptyList() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataForDate("PUBLISHER_11", "20170101");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(0, list.size());
  }

  @Test
  public void testGetAllDataForDateRangeWithExistingDateRangeShouldReturnData() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataForDateRange("PUBLISHER_11", "20180101", "20180103");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(12, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_MOBILE_FILTERED"));

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_CLICK_MOBILE_FILTERED"));
  }

  @Test
  public void testGetAllDataForDateRangeWithNonExistingDateRangeShouldReturnEmptyList() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataForDateRange("PUBLISHER_11", "20170101", "20170103");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(0, list.size());
  }

  @Test
  public void testGetAllDataGreaterThanDateWithExistingDateShouldReturnData() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataGreaterThanDate("PUBLISHER_11", "20180101");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(6, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-02_CLICK_MOBILE_FILTERED"));
  }

  @Test
  public void testGetAllDataLessThanDateWithExistingDateShouldReturnData() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataLessThanDate("PUBLISHER_11", "20180102");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(6, list.size());

    Assert.assertEquals(6, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2018-01-01_CLICK_MOBILE_FILTERED"));
  }

  private static void prepareTestData(Bucket bucket) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.YEAR, 2018);
    calendar.set(Calendar.MONTH, 0);
    calendar.set(Calendar.DAY_OF_MONTH, 2);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);

    // dt1 and dt represent the date range to generate test data.
    Date dt2 = calendar.getTime();
    calendar.set(Calendar.DAY_OF_MONTH, 1);
    Date dt1 = calendar.getTime();

    final String prefix = "PUBLISHER_11";
    final String[] actions = {"CLICK", "IMPRESSION", "VIEWABLE"};

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    while (dt1.getTime() <= dt2.getTime()) {
      String currentDate = simpleDateFormat.format(dt1);
      for (String action : actions) {
        String key1 = generateKey(prefix, currentDate, action, true, true);
        String key2 = generateKey(prefix, currentDate, action, true, false);
        String key3 = generateKey(prefix, currentDate, action, false, true);
        String key4 = generateKey(prefix, currentDate, action, false, false);

        Calendar delta = Calendar.getInstance();
        delta.setTime(dt1);
        Date deltaTime = delta.getTime();

        // Each day has similar data.
        JsonArray jsonArray = JsonArray.create();
        int numberOfRecordPerDay = 5;
        while (numberOfRecordPerDay > 0) {
          JsonObject jsonObject = JsonObject.create();
          jsonObject.put("count", 1);
          jsonObject.put("timestamp", deltaTime.getTime());
          jsonArray.add(jsonObject);
          numberOfRecordPerDay--;
          delta.add(Calendar.MINUTE, 15);
          deltaTime = delta.getTime();
        }

        bucket.upsert(JsonArrayDocument.create(key1, jsonArray));
        bucket.upsert(JsonArrayDocument.create(key2, jsonArray));
        bucket.upsert(JsonArrayDocument.create(key3, jsonArray));
        bucket.upsert(JsonArrayDocument.create(key4, jsonArray));
      }

      calendar.add(Calendar.DAY_OF_MONTH, 1);
      dt1 = calendar.getTime();
    }
  }

  private static String generateKey(String prefix, String date, String action, boolean isMobile, boolean isFiltered) {
    String key = prefix + "_" + date + "_" + action;
    if (isMobile && isFiltered) {
      key += "_MOBILE_FILTERED";
    } else if (isMobile && !isFiltered) {
      key += "_MOBILE_RAW";
    } else if (!isMobile && isFiltered) {
      key += "_DESKTOP_FILTERED";
    } else {
      key += "_DESKTOP_RAW";
    }
    return key;
  }
}
