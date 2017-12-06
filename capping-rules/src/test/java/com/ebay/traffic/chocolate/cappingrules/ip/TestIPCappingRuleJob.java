package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.cappingrules.HBaseConnection;
import com.ebay.traffic.chocolate.cappingrules.HBaseScanIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
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
  final static String TRANSACTION_TABLE_NAME = "prod_transactional";
  final static String CAPPINGRESULT_TABLE_NAME = "capping_result";
  final static String TRANSACTION_CF_DEFAULT = "x";

  private static HBaseTestingUtility hbaseUtility;
  private static Configuration hbaseConf;
  private static Connection hbaseConnection;
  private static HTable transactionalTable;
  private static HBaseAdmin hbaseAdmin;
  private static ZonedDateTime yesterday = ZonedDateTime.now().minusDays(1);
  private static ZonedDateTime today = yesterday.plusDays(1);
  private static IPCappingRuleJob job;

  @BeforeClass
  public static void setUp() throws Exception {
    hbaseUtility = new HBaseTestingUtility();
    hbaseUtility.startMiniCluster();

    hbaseConf = hbaseUtility.getConfiguration();
    hbaseConnection = hbaseUtility.getConnection();
    hbaseAdmin = hbaseUtility.getHBaseAdmin();
    transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), hbaseConnection);
    job = new IPCappingRuleJob("TestIPCappingRuleJob", "local[4]", TRANSACTION_TABLE_NAME, CAPPINGRESULT_TABLE_NAME, today
      .toInstant().toEpochMilli(), today.toInstant().toEpochMilli() - yesterday.toInstant().toEpochMilli(), 4*60*60*1000, 8);
    HBaseConnection.setConfiguration(hbaseConf);
    IPCappingRuleJob.setMod((short) 3);
    initHBaseTransactionTable();
    initHBaseCappingResultTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    hbaseUtility.shutdownMiniCluster();
  }

  @Test
  public void testHBaseScanIterator() throws Exception {
    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Result result = null;
    int numberOfRow = 0;
    while (iter.hasNext()) {
      result = iter.next();
      numberOfRow++;
    }
    Assert.assertEquals(numberOfRow, 37);
    iter.close();
  }

  @Test
  public void testParseOptions() throws Exception {
    String[] args = {"--jobName",  "IPCappingRule", "--mode", "yarn", "--table",  "prod_transactional",
      "--resultTable", "capping_result", "--time", "12345", "--timeRange", "123", "--timeWindow", "1800000", "--threshold", "1000"};
    CommandLine cmd = IPCappingRuleJob.parseOptions(args);
    Assert.assertEquals("IPCappingRule", cmd.getOptionValue("jobName"));
    Assert.assertEquals("yarn", cmd.getOptionValue("mode"));
    Assert.assertEquals("prod_transactional", cmd.getOptionValue("table"));
    Assert.assertEquals("12345", cmd.getOptionValue("time"));
    Assert.assertEquals("123", cmd.getOptionValue("timeRange"));
    Assert.assertEquals("1800000", cmd.getOptionValue("timeWindow"));
    Assert.assertEquals("1000", cmd.getOptionValue("threshold"));
  }

  @Test
  public void testIPCappingRuleJob() throws Exception {
    job.run();
    HBaseScanIterator resultTableItr = new HBaseScanIterator("capping_result");
    Assert.assertEquals(16, getCount(resultTableItr));
    resultTableItr = new HBaseScanIterator("capping_result");
    Assert.assertEquals(10, getFailedCount(resultTableItr));
    job.stop();
  }

  @Test
  public void testGetTimestampFromIdentifier() {
    long time = 1512527672000l;
    byte[] identifier = job.generateIdentifier(time, (short)3);
    long timestamp = job.getTimestampFromIdentifier(identifier);
    Assert.assertEquals(time, timestamp);
  }

  protected int getFailedCount(HBaseScanIterator iter) {
    int count = 0;
    while(iter.hasNext()) {
      Result result = iter.next();
      String failedRule = Bytes.toString(result.getValue(Bytes.toBytes("x"), Bytes.toBytes("capping_failed_rule")));
      if(failedRule.equals("IPCappingRule")) {
        count ++;
      }
    }
    return count;
  }

  protected int getCount(HBaseScanIterator iter) {
    int numberOfRow = 0;
    while (iter.hasNext()) {
      iter.next();
      numberOfRow++;
    }
    return numberOfRow;
  }

  private static byte[] generateIdentity(int hour, int minute, short modValue) {
    return job.generateIdentifier(yesterday.plusHours(hour).plusMinutes(minute).toInstant().toEpochMilli(), modValue);
  }

  private static void initHBaseTransactionTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
      .setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);

    String requestHeaderValid = "Cookie: aaa ;|X-eBay-Client-IP: 50.206.232.22|Connection: keep-alive|User-Agent: " +
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 " +
      "Safari/537.36";
    String requestHeaderInvalid = "Cookie: aaa ;|X-eBay-Client-IP: 11.11.11.11|Connection: keep-alive|User-Agent: " +
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 " +
      "Safari/537.36";

    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20,0, (short) 0), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20,1, (short) 0), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20,2, (short) 0), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 3, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 4, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 5, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 6, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 7, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 8, (short) 0), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 9, (short) 0), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 10, (short) 0), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 11, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 12, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 13, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 14, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(24, 1, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(19, 0, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(19, 1, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(19, 2, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(19, 3, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(19, 4, (short) 0), "CLICK", requestHeaderInvalid));

    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 0, (short) 1), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 1, (short) 1), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 2, (short) 1), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 3, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 4, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 5, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 6, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 7, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 8, (short) 1), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 9, (short) 1), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 10, (short) 1), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 11, (short) 1), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 12, (short) 1), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(20, 13, (short) 1), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(14,0, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(25,0, (short) 0), "IMPRESSION", requestHeaderInvalid));
  }

  private static void initHBaseCappingResultTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(CAPPINGRESULT_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
      .setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);
  }

  private static <T> void putCell(Put put, String family, String qualifier, T value) {

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
    } else if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else {
      bytes = null;
    }

    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes);
  }

  private static void addEvent(HTable table, IPCappingEvent event) throws IOException {

    Put put = new Put(event.getIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", event.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", event.getRequestHeaders());
    table.put(put);
  }
}