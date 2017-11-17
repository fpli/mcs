package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.cappingrules.HBaseConnection;
import com.ebay.traffic.chocolate.cappingrules.HBaseScanIterator;
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
  private ZonedDateTime yesterday = ZonedDateTime.now().minusDays(1);
  private ZonedDateTime today = yesterday.plusDays(1);
  private IPCappingRuleJob job;

  @Before
  public void setUp() throws Exception {
    hbaseUtility = new HBaseTestingUtility();
    hbaseUtility.startMiniCluster();

    hbaseConf = hbaseUtility.getConfiguration();
    hbaseConnection = hbaseUtility.getConnection();
    hbaseAdmin = hbaseUtility.getHBaseAdmin();
    transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), hbaseConnection);
    job = new IPCappingRuleJob("TestIPCappingRuleJob", "local[4]", TRANSACTION_TABLE_NAME, today
      .toInstant().toEpochMilli(), today.toInstant().toEpochMilli() - yesterday.toInstant().toEpochMilli(), 8);
    HBaseConnection.setConfiguration(hbaseConf);
    IPCappingRuleJob.setMod((short) 3);
    initHBaseTransactionTable();
    initHBaseCappingResultTable();
  }

  @After
  public void tearDown() throws Exception {
    hbaseUtility.shutdownMiniCluster();
  }

  @Test
  public void testHBaseScanIterator() throws Exception {
    HBaseConnection.setConfiguration(hbaseConf);
    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Result result = null;
    int numberOfRow = 0;
    while (iter.hasNext()) {
      result = iter.next();
      numberOfRow++;
    }
    Assert.assertEquals(numberOfRow, 12);
    iter.close();
  }

  @Test
  public void testIPCappingRuleJob() throws Exception {
    job.run();
    Dataset<Row> result = job.readEvents("capping_result");
    result.show();
    Assert.assertEquals(10, result.count());
    Dataset<Row> modifiedResult = result.filter(result.col("cappingPassed").equalTo(false));
    Assert.assertEquals(10, modifiedResult.count());
    job.stop();
  }

  private byte[] generateIdentity(int hour, short modValue) {
    return job.generateIdentifier(yesterday.plusHours(hour).toInstant().toEpochMilli(), modValue);
  }

  private void initHBaseTransactionTable() throws IOException {
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

    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(0, (short) 0), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(1, (short) 0), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(2, (short) 0), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(3, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(4, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(5, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(6, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(7, (short) 0), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(8, (short) 0), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(9, (short) 0), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(10, (short) 0), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(11, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(12, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(13, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(14, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(25, (short) 0), "IMPRESSION", requestHeaderInvalid));

    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(0, (short) 1), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(1, (short) 1), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(2, (short) 1), "IMPRESSION", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(3, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(4, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(5, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(6, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(7, (short) 1), "CLICK", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(8, (short) 1), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(9, (short) 1), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(10, (short) 1), "CLICK", requestHeaderValid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(11, (short) 1), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(12, (short) 1), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(13, (short) 1), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(14, (short) 0), "IMPRESSION", requestHeaderInvalid));
    addEvent(transactionalTable, new IPCappingEvent(generateIdentity(25, (short) 0), "IMPRESSION", requestHeaderInvalid));
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
    } else if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else {
      bytes = null;
    }

    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes);
  }

  private void addEvent(HTable table, IPCappingEvent event) throws IOException {

    Put put = new Put(event.getIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", event.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", event.getRequestHeaders());
    table.put(put);
  }
}