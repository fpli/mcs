package com.ebay.traffic.chocolate.reportsvc.dao;

import com.ebay.traffic.chocolate.mkttracksvc.dao.CouchbaseClientMock;
import com.ebay.traffic.chocolate.reportsvc.TestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.util.*;

public class ReportDaoTest {

  @BeforeClass
  public static void construct() throws Exception {
    CouchbaseClientMock.createClient("report");
    TestHelper.prepareTestData(CouchbaseClientMock.getBucket(), new String[]{"PUBLISHER_11"});
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
      list = reportDao.getAllDataForMonth("PUBLISHER_11", "200001");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(20, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_RAW"));

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_VIEWABLE_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_MOBILE_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_MOBILE_RAW"));
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
      list = reportDao.getAllDataForDate("PUBLISHER_11", "20000101");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(10, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_RAW"));
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
      list = reportDao.getAllDataForDateRange("PUBLISHER_11", "20000101", "20000103");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(20, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_RAW"));

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_VIEWABLE_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_MOBILE_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_MOBILE_RAW"));
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
      list = reportDao.getAllDataGreaterThanDate("PUBLISHER_11", "20000101");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(10, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_VIEWABLE_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_CLICK_MOBILE_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-02_IMPRESSION_MOBILE_RAW"));
  }

  @Test
  public void testGetAllDataLessThanDateWithExistingDateShouldReturnData() {
    ReportDao reportDao = new ReportDaoImpl(CouchbaseClientMock.getBucket());
    List<ReportDo> list = null;
    try {
      list = reportDao.getAllDataLessThanDate("PUBLISHER_11", "20000102");
    } catch (ParseException e) {
      Assert.fail();
    }

    Assert.assertEquals(10, list.size());

    Set<String> keySet = new HashSet<>();
    for (ReportDo reportDo : list) {
      Assert.assertEquals(5, reportDo.getDocument().size());
      keySet.add(reportDo.getKey());
    }

    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_VIEWABLE_DESKTOP_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_CLICK_MOBILE_RAW"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_FILTERED"));
    Assert.assertTrue(keySet.contains("PUBLISHER_11_2000-01-01_IMPRESSION_MOBILE_RAW"));
  }
}
