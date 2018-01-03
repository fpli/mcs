package com.ebay.traffic.chocolate.cappingrules;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.ebay.traffic.chocolate.cappingrules.cassandra.CassandraClient;
import com.ebay.traffic.chocolate.cappingrules.cassandra.ReportDataGenerator;
import com.ebay.traffic.chocolate.cappingrules.constant.CassandraConstant;
import com.ebay.traffic.chocolate.cappingrules.constant.Env;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import com.ebay.traffic.chocolate.cappingrules.constant.StorageType;
import com.ebay.traffic.chocolate.cappingrules.dto.FilterResultEvent;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by yimeng on 11/26/17.
 */

public class TestReportDataGenerator extends AbstractCappingRuleTest {
  private static final long TEST_CAMPAIGN_ID = 7000005263L;
  private static final long TEST_PARTNER_ID = 7000000001L;
  private static String stopTime;
  private static Calendar testDataCalendar = Calendar.getInstance();
  private static String CQL_CAMPAIGN = "select * from campaign_report where campaign_id = %d and month = %d and day = %d and timestamp = %d";
  
  @BeforeClass
  public static void initialHbaseTable() throws IOException {
    setDataIntoTransactionTable();
    
    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Assert.assertEquals(48, getCount(iter));
    iter.close();
    
    stopTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(testDataCalendar.getTime());
    //testDataCalendar.add(Calendar.MINUTE, -10);
  }
  
  /**
   * Test store report data to Cassandra
   * <br/>
   * could be commented if only want to run without QA cassandra connection
   *
   * @throws Exception
   */
  @Test
  public void testSaveToCassandraQA() throws Exception {
    String env = Env.QA.name();
    ReportDataGenerator job = new ReportDataGenerator("TestReportDataGenerator", "local[4]",
        TRANSACTION_TABLE_NAME, RESULT_TABLE_NAME, "EPN", stopTime, 30, StorageType.CASSANDRA.name(), env);
    job.run();
  
    //Validation
    CassandraClient cassandraClient = CassandraClient.getInstance(env);
    Session cassandraSession = cassandraClient.getSession(Env.QA.name());
  
    int month = Integer.valueOf(IdentifierUtil.MONTH_FORMAT.format(testDataCalendar.getTimeInMillis()));
    int day = Integer.valueOf(IdentifierUtil.DATE_FORMAT.format(testDataCalendar.getTimeInMillis()));
    long timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000;
  
    String cqlStatement = String.format(CQL_CAMPAIGN, 76543210001l, month, day, timestamp);
    ResultSet rs = cassandraSession.execute(cqlStatement);
    assertCassandrData(rs.one(), month, day, timestamp, 10, 4, 9, 4, 5,3 ,7 ,4);
//
//    timestamp += 1776;
//    cqlStatement = String.format(CQL_CAMPAIGN, 76543210002l, month, day, timestamp);
//    rs = cassandraSession.execute(cqlStatement);
//    assertCassandrData(rs.one(), month, day, timestamp, 11, 5, 0, 0, 0,0 ,3 ,0);
//
//    timestamp += 1555;
//    cqlStatement = String.format(CQL_CAMPAIGN, 76543210003l, month, day, timestamp);
//    rs = cassandraSession.execute(cqlStatement);
//    assertCassandrData(rs.one(), month, day, timestamp, 13, 7, 0, 0, 0,0 ,3 ,0);
//
//    timestamp += 1578;
//    cqlStatement = String.format(CQL_CAMPAIGN, 76543210003l, month, day, timestamp);
//    rs = cassandraSession.execute(cqlStatement);
//    assertCassandrData(rs.one(), month, day, timestamp, 11, 5, 0, 0, 0,0 ,3 ,0);
//
//    timestamp += 1776;
//    cqlStatement = String.format(CQL_CAMPAIGN, 76543210002l, month, day, timestamp);
//    rs = cassandraSession.execute(cqlStatement);
//    assertCassandrData(rs.one(), month, day, timestamp, 11, 5, 0, 0, 0,0 ,3 ,0);
  }
  
  private void assertCassandrData(Row row, Integer month, Integer day, Long timestamp, Integer grossClick, Integer clicks,
                             Integer grossImp,  Integer imp, Integer grossViewImp, Integer viewImp, Integer mobileClick, Integer mobileImp) {
    Assert.assertEquals(Long.valueOf(month),Long.valueOf(row.getInt(CassandraConstant.MONTH_COLUMN)));
    Assert.assertEquals(Long.valueOf(day), Long.valueOf(row.getInt(CassandraConstant.DAY_COLUMN)));
    Assert.assertEquals(Long.valueOf(timestamp), Long.valueOf(row.getLong(CassandraConstant.TIMESTAMP)));
    Assert.assertEquals(Long.valueOf(clicks), Long.valueOf(row.getInt(CassandraConstant.CLICKS_COLUMN)));
    Assert.assertEquals(Long.valueOf(grossClick), Long.valueOf(row.getInt(CassandraConstant.GROSS_CLICKS_COLUMN)));
    Assert.assertEquals(Long.valueOf(imp), Long.valueOf(row.getInt(CassandraConstant.IMPRESSIONS_COLUMN)));
    Assert.assertEquals(Long.valueOf(grossImp), Long.valueOf(row.getInt(CassandraConstant.GROSS_IMPRESSIONS_COLUMN)));
    Assert.assertEquals(Long.valueOf(viewImp), Long.valueOf(row.getInt(CassandraConstant.VIEWABLE_IMPRESSIONS_COLUMN)));
    Assert.assertEquals(Long.valueOf(grossViewImp), Long.valueOf(row.getInt(CassandraConstant.GROSS_VIEWABLE_IMPRESSIONS_COLUMN)));
    Assert.assertEquals(Long.valueOf(mobileClick), Long.valueOf(row.getInt(CassandraConstant.MOBILE_CLICKS_COLUMN)));
    Assert.assertEquals(Long.valueOf(mobileImp), Long.valueOf(row.getInt(CassandraConstant.MOBILE_IMPRESSIONS_COLUMN)));
  }

  /**
   * Test store report data to Cassandra
   * <br/>
   * could be commented if only want to run without QA cassandra connection
   *
   * @throws Exception
   */
  @Test
  public void testSaveToCassandraEmbedded() throws Exception {
//
//    ReportDataGenerator job = new ReportDataGenerator("TestReportDataGenerator", "local[4]",
//        TRANSACTION_TABLE_NAME, RESULT_TABLE_NAME, "EPN", stopTime, 30, StorageType.CASSANDRA.name(), "QA");
//    job.run();

  }
  

  /**
   * Test store report data to HBase
   *
   * @throws Exception
   */
  @Test
  public void testSaveToHbase() throws Exception {

    ReportDataGenerator job = new ReportDataGenerator("TestReportDataGenerator", "local[4]",
        TRANSACTION_TABLE_NAME, RESULT_TABLE_NAME, "EPN", stopTime, 30, StorageType.HBASE.name(), null);
    job.run();

    HBaseScanIterator resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME);
    int numberOfRow = 0;
    Result result = null;
    int month = Integer.valueOf(IdentifierUtil.MONTH_FORMAT.format(testDataCalendar.getTimeInMillis()));
    int day = Integer.valueOf(IdentifierUtil.DATE_FORMAT.format(testDataCalendar.getTimeInMillis()));
    long timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000;
    System.out.println("====ExpectedTimestamp====== " + timestamp);
    while (resultTableItr.hasNext()) {
      result = resultTableItr.next();
      long id = Bytes.toLong(result.getRow());
      if (id == 76543210001l) {
        timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000;
        assertHbaseData(result, month, day, timestamp, 10, 4, 9, 4, 5, 3, 7 ,4);
      } else if (id == 76543210002l) {
        timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000 + 1500;
        assertHbaseData(result, month, day, timestamp, 11, 5, 0, 0, 0, 0, 3, 0);
      } else if (id == 76543210003l) {
        timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000 + 3000;
        assertHbaseData(result, month, day, timestamp, 13, 7, 0, 0, 0, 0, 3, 0);
      } else if (id == 1234560001l) {
        timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000;
        assertHbaseData(result, month, day, timestamp, 21, 9, 9, 4, 5, 3, 10, 4);
      } else if (id == 1234560002l) {
        timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000 + 1500 + 1500;
        assertHbaseData(result, month, day, timestamp, 13, 7, 0, 0, 0, 0, 3, 0);
      }
      numberOfRow++;
    }
    Assert.assertEquals(5, numberOfRow);
    resultTableItr.close();

    job.stop();
  }
  
  private void assertHbaseData(Result result, Integer month, Integer day, Long timestamp, Integer grossClick, Integer clicks,
                               Integer grossImp,  Integer imp, Integer grossViewImp, Integer viewImp, Integer mobileClick, Integer mobileImp){
    Assert.assertEquals(Long.valueOf(month),Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("month")))));
    Assert.assertEquals(Long.valueOf(day), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("day")))));
    Assert.assertEquals(Long.valueOf(timestamp), Long.valueOf(Bytes.toLong(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("timestamp")))));
    Assert.assertEquals(Long.valueOf(grossClick), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("gross_clicks")))));
    Assert.assertEquals(Long.valueOf(clicks), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("clicks")))));
    Assert.assertEquals(Long.valueOf(grossImp), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("gross_impressions")))));
    Assert.assertEquals(Long.valueOf(imp), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("impressions")))));
    Assert.assertEquals(Long.valueOf(grossViewImp), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("gross_view_impressions")))));
    Assert.assertEquals(Long.valueOf(viewImp), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("view_impressions")))));
    Assert.assertEquals(Long.valueOf(mobileClick), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("mobile_clicks")))));
    Assert.assertEquals(Long.valueOf(mobileImp), Long.valueOf(Bytes.toInt(result.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("mobile_impressions")))));
  }
  
  protected static void setDataIntoTransactionTable() throws IOException {
    
    // CampaignId = 76543210001, PartnerId = 1234560001
    long timestamp = testDataCalendar.getTimeInMillis() - 10 * 60 * 1000;
    long snapshotId = IdentifierUtil.getSnapshotId(timestamp, 0);
    short mod = 1;
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp, mod), snapshotId,
        76543210001l, 1234560001l, "EPN", "CLICK", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 1, mod),
        snapshotId + 1, 76543210001l, 1234560001l, "EPN", "CLICK", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 2, mod),
        snapshotId + 2, 76543210001l, 1234560001l, "EPN", "CLICK", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 3, mod),
        snapshotId + 3, 76543210001l, 1234560001l, "EPN", "CLICK", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 4, mod),
        snapshotId + 4, 76543210001l, 1234560001l, "EPN", "CLICK", false, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 5, mod),
        snapshotId + 5, 76543210001l, 1234560001l, "EPN", "CLICK", false, false, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 6, mod),
        snapshotId + 6, 76543210001l, 1234560001l, "EPN", "CLICK", false, false, false, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 7, mod),
        snapshotId + 7, 76543210001l, 1234560001l, "EPN", "CLICK", true, false, false, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 8, mod),
        snapshotId + 8, 76543210001l, 1234560001l, "EPN", "CLICK", true, true, false, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 9, mod),
        snapshotId + 9, 76543210001l, 1234560001l, "EPN", "CLICK", false, true, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 10, mod),
        snapshotId + 10, 76543210001l, 1234560001l, "EPN", "IMPRESSION", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 11, mod),
        snapshotId + 11, 76543210001l, 1234560001l, "EPN", "IMPRESSION", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 12, mod),
        snapshotId + 12, 76543210001l, 1234560001l, "EPN", "IMPRESSION", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 13, mod),
        snapshotId + 13, 76543210001l, 1234560001l, "EPN", "IMPRESSION", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 14, mod),
        snapshotId + 14, 76543210001l, 1234560001l, "EPN", "IMPRESSION", true, true, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 15, mod),
        snapshotId + 15, 76543210001l, 1234560001l, "EPN", "IMPRESSION", true, true, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 16, mod),
        snapshotId + 16, 76543210001l, 1234560001l, "EPN", "IMPRESSION", false, false, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 17, mod),
        snapshotId + 17, 76543210001l, 1234560001l, "EPN", "IMPRESSION", false, false, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 18, mod),
        snapshotId + 18, 76543210001l, 1234560001l, "EPN", "IMPRESSION", false, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 19, mod),
        snapshotId + 19, 76543210001l, 1234560001l, "EPN", "VIEWABLE", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 20, mod),
        snapshotId + 20, 76543210001l, 1234560001l, "EPN", "VIEWABLE", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 21, mod),
        snapshotId + 21, 76543210001l, 1234560001l, "EPN", "VIEWABLE", true, false, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 22, mod),
        snapshotId + 22, 76543210001l, 1234560001l, "EPN", "VIEWABLE", false, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 23, mod),
        snapshotId + 23, 76543210001l, 1234560001l, "EPN", "VIEWABLE", true, true, true, false));
    
    // CampaignId = 76543210002, PartnerId = 1234560001
    timestamp = timestamp + 1500;
    snapshotId = IdentifierUtil.getSnapshotId(timestamp, 0);
    //mod = 2;
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp, mod), snapshotId,
        76543210002l, 1234560001l, "EPN", "CLICK", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 1, mod),
        snapshotId + 1, 76543210002l, 1234560001l, "EPN", "CLICK", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 2, mod),
        snapshotId + 2, 76543210002l, 1234560001l, "EPN", "CLICK", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 3, mod),
        snapshotId + 3, 76543210002l, 1234560001l, "EPN", "CLICK", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 4, mod),
        snapshotId + 4, 76543210002l, 1234560001l, "EPN", "CLICK", false, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 5, mod),
        snapshotId + 5, 76543210002l, 1234560001l, "EPN", "CLICK", false, false, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 6, mod),
        snapshotId + 6, 76543210002l, 1234560001l, "EPN", "CLICK", false, false, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 7, mod),
        snapshotId + 7, 76543210002l, 1234560001l, "EPN", "CLICK", true, false, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 8, mod),
        snapshotId + 8, 76543210002l, 1234560001l, "EPN", "CLICK", true, true, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 9, mod),
        snapshotId + 9, 76543210002l, 1234560001l, "EPN", "CLICK", false, true, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 10, mod),
        snapshotId + 10, 76543210002l, 1234560001l, "EPN", "CLICK", true, true, true, false));
    
    // CampaignId = 76543210003, PartnerId = 1234560002
    timestamp = timestamp + 1500;
    snapshotId = IdentifierUtil.getSnapshotId(timestamp, 0);
    //mod = 3;
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp, mod), snapshotId,
        76543210003l, 1234560002l, "EPN", "CLICK", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 1, mod),
        snapshotId + 1, 76543210003l, 1234560002l, "EPN", "CLICK", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 2, mod),
        snapshotId + 2, 76543210003l, 1234560002l, "EPN", "CLICK", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 3, mod),
        snapshotId + 3, 76543210003l, 1234560002l, "EPN", "CLICK", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 4, mod),
        snapshotId + 4, 76543210003l, 1234560002l, "EPN", "CLICK", false, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 5, mod),
        snapshotId + 5, 76543210003l, 1234560002l, "EPN", "CLICK", false, false, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 6, mod),
        snapshotId + 6, 76543210003l, 1234560002l, "EPN", "CLICK", false, false, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 7, mod),
        snapshotId + 7, 76543210003l, 1234560002l, "EPN", "CLICK", true, false, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 8, mod),
        snapshotId + 8, 76543210003l, 1234560002l, "EPN", "CLICK", true, true, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 9, mod),
        snapshotId + 9, 76543210003l, 1234560002l, "EPN", "CLICK", false, true, false, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 10, mod),
        snapshotId + 10, 76543210003l, 1234560002l, "EPN", "CLICK", true, true, true, false));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 11, mod),
        snapshotId + 11, 76543210003l, 1234560002l, "EPN", "CLICK", true, true, true, true));
    addEvent(transactionalTable, new FilterResultEvent(IdentifierUtil.generateIdentifier(timestamp + 12, mod),
        snapshotId + 12, 76543210003l, 1234560002l, "EPN", "CLICK", true, true, true, false));
    
  }
  
  private static void addEvent(HTable table, FilterResultEvent resultEvent) throws IOException {
    Put put = new Put(resultEvent.getRowIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "snapshot_id", resultEvent.getSnapshotId());
    putCell(put, TRANSACTION_CF_DEFAULT, "campaign_id", resultEvent.getCampaignId());
    putCell(put, TRANSACTION_CF_DEFAULT, "partner_id", resultEvent.getPartnerId());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_type", resultEvent.getChannelType());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", resultEvent.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "filter_passed", resultEvent.getFilterPassed());
    putCell(put, TRANSACTION_CF_DEFAULT, "capping_passed", resultEvent.getCappingPassed());
    putCell(put, TRANSACTION_CF_DEFAULT, "is_impressed", resultEvent.getImpressed());
    putCell(put, TRANSACTION_CF_DEFAULT, "is_mobile", resultEvent.getMobile());
    table.put(put);
  }
}
