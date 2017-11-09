package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.cappingrules.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.ZonedDateTime;

/**
 * Unit test for IPCAppingRuleJob
 *
 * @author xiangli4
 */

public class TestIPCappingRuleJob {
  final String TRANSACTION_TABLE_NAME = "prod_transactional";
  final String CAPPINGRESULT_TABLE_NAME = "capping_result";
  final String TRANSACTION_CF_DEFAULT = "x";

  private HBaseTestingUtility hbaseUtility;
  private Configuration hbaseConf;
  private Connection hbaseConnection;
  private HTable transactionalTable;
  private HBaseAdmin hbaseAdmin;
  private long TIME_MASK = 0xFFFFFFl << 40l;
  private ZonedDateTime yesterday = ZonedDateTime.now().minusDays(1);
  private ZonedDateTime today = yesterday.plusDays(1);

  @Before
  public void setUp() throws Exception {
    hbaseUtility = new HBaseTestingUtility();
    hbaseUtility.startMiniCluster();

    hbaseConf = hbaseUtility.getConfiguration();
    hbaseConnection = hbaseUtility.getConnection();
    hbaseAdmin = hbaseUtility.getHBaseAdmin();
    transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), hbaseConnection);

    initHBaseTransactionTable();
    initHBaseCappingResultTable();
  }

  @After
  public void tearDown() throws Exception {
    hbaseUtility.shutdownMiniCluster();
  }

  @Test
  public void testIPCappingRuleJob() throws Exception {
    IPCappingRuleJob job = new IPCappingRuleJob("TestIPCappingRuleJob", "local[4]", TRANSACTION_TABLE_NAME, today.toInstant().toEpochMilli(), today.toInstant().toEpochMilli() - yesterday.toInstant().toEpochMilli(), 3);
    job.setHBaseConf(hbaseConf);
    job.run();
    Dataset<Row> result = job.readEvents("capping_result");
    Assert.assertEquals(4, result.count());
    Dataset<Row> modifiedResult = result.filter(result.col("filterPassed").equalTo(false));
    Assert.assertEquals(4, modifiedResult.count());
    job.stop();
  }

  private void initHBaseTransactionTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
      .setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);

    String requestHeaderValid = "Cookie: aaa ;|X-eBay-Client-IP: 50.206.232.22|Connection: keep-alive|User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36";
    String requestHeaderInvalid = "Cookie: aaa ;|X-eBay-Client-IP: 11.11.11.11|Connection: keep-alive|User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36";

    addEvent(transactionalTable, new Event((yesterday.toInstant().toEpochMilli() & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 1, 100, requestHeaderValid, true, "None", true));
    addEvent(transactionalTable, new Event((yesterday.plusMinutes(1).toInstant().toEpochMilli() & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 2, 101, requestHeaderValid, true, "None", true));
    addEvent(transactionalTable, new Event((yesterday.plusMinutes(2).toInstant().toEpochMilli() & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 3, 101, requestHeaderValid, true, "None", true));
    addEvent(transactionalTable, new Event((yesterday.plusHours(1).toInstant().toEpochMilli() & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 4, 101, requestHeaderInvalid, true, "None", true));
    addEvent(transactionalTable, new Event((yesterday.plusHours(2).toInstant().toEpochMilli() & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 5, 101, requestHeaderInvalid, true, "None", true));
    addEvent(transactionalTable, new Event((yesterday.plusHours(3).toInstant().toEpochMilli() & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 6, 102, requestHeaderInvalid, true, "None", true));
    addEvent(transactionalTable, new Event((yesterday.plusHours(23).toInstant().toEpochMilli() & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 7, 103, requestHeaderInvalid, true, "None", true));
    addEvent(transactionalTable, new Event(((yesterday.plusHours(25).toInstant().toEpochMilli()) & ~TIME_MASK) << 24l, yesterday.toInstant().toEpochMilli(), 0, 8, 104, requestHeaderInvalid, true, "None", true));
  }

  private void initHBaseCappingResultTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(CAPPINGRESULT_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
      .setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);
  }

  private <T> void putCell(Put put, String family, String qualifier, T value) {

    byte[] bytes;
    if (value instanceof Long) {
      bytes = Bytes.toBytes(((Long) value).longValue());
    } else if (value instanceof String) {
      bytes = Bytes.toBytes((String) value);
    } else if (value instanceof Boolean) {
      bytes = Bytes.toBytes(((Boolean) value).booleanValue());
    } else if (value instanceof Timestamp) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      String str = ((Timestamp) value).toString();
      int bufferSize = str.getBytes().length;
      System.arraycopy(str.getBytes(), 0, buffer.array(), 0, bufferSize);
      bytes = buffer.array();
    } else {
      bytes = Bytes.toBytes(value.toString());
    }

    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes);
  }

  private void addEvent(HTable table, Event event) throws IOException {
    Put put = new Put(Bytes.toBytes(event.getSnapshotId()));
    putCell(put, TRANSACTION_CF_DEFAULT, "request_timestamp", event.getTimestamp());
    putCell(put, TRANSACTION_CF_DEFAULT, "publisher_id", event.getPublisherId());
    putCell(put, TRANSACTION_CF_DEFAULT, "campaign_id", event.getCampaignId());
    putCell(put, TRANSACTION_CF_DEFAULT, "snid", event.getSnid());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", event.getRequestHeaders());
    putCell(put, TRANSACTION_CF_DEFAULT, "is_tracked", event.isTracked());
    putCell(put, TRANSACTION_CF_DEFAULT, "filter_failed_rule", event.getFilterFailedRule());
    putCell(put, TRANSACTION_CF_DEFAULT, "filter_passed", event.isFilterPassed());
    table.put(put);
  }
}