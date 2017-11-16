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

import java.io.ByteArrayOutputStream;
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
    IPCappingRuleJob job = new IPCappingRuleJob("TestIPCappingRuleJob", "local[4]", TRANSACTION_TABLE_NAME, today
      .toInstant().toEpochMilli(), today.toInstant().toEpochMilli() - yesterday.toInstant().toEpochMilli(), 8);
    HBaseConnection.setConfiguration(hbaseConf);
    IPCappingRuleJob.setMod((short)3);
    job.run();
    Dataset<Row> result = job.readEvents("capping_result");
    result.show();
    Assert.assertEquals(10, result.count());
    Dataset<Row> modifiedResult = result.filter(result.col("cappingPassed").equalTo(false));
    Assert.assertEquals(10, modifiedResult.count());
    job.stop();
  }

  @Test
  public void testTimeStamp() {
    long timestamp = 1510755792123l;

    for (short i = 0; i < 293; i++) {
      byte[] bytes = Bytes.toBytes(i);
      System.out.println(bytes.length);
      for (int j = 0; j < bytes.length; j++) {
        System.out.println(Integer.toBinaryString(bytes[j] & 255 | 256).substring(1));
      }
    }
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

    addEvent(transactionalTable, new IPCappingEvent((yesterday.toInstant().toEpochMilli() & ~TIME_MASK) << 24l,
      "IMPRESSION", requestHeaderValid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusMinutes(1).toInstant().toEpochMilli() &
      ~TIME_MASK) << 24l, "IMPRESSION", requestHeaderValid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusMinutes(2).toInstant().toEpochMilli() &
      ~TIME_MASK) << 24l, "IMPRESSION", requestHeaderValid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(1).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(2).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(3).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(4).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(5).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(6).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderValid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(7).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderValid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(8).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderValid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(9).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(10).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(11).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(23).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 0);
    addEvent(transactionalTable, new IPCappingEvent(((yesterday.plusHours(25).toInstant().toEpochMilli()) &
      ~TIME_MASK) << 24l, "IMPRESSION", requestHeaderInvalid, true), 0);

    addEvent(transactionalTable, new IPCappingEvent((yesterday.toInstant().toEpochMilli() & ~TIME_MASK) << 24l,
      "IMPRESSION", requestHeaderValid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusMinutes(1).toInstant().toEpochMilli() &
      ~TIME_MASK) << 24l, "IMPRESSION", requestHeaderValid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusMinutes(2).toInstant().toEpochMilli() &
      ~TIME_MASK) << 24l, "IMPRESSION", requestHeaderValid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(1).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(2).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(3).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(4).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(5).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(6).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderValid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(7).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderValid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(8).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "CLICK", requestHeaderValid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(9).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(10).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(11).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent((yesterday.plusHours(23).toInstant().toEpochMilli() & ~TIME_MASK)
      << 24l, "IMPRESSION", requestHeaderInvalid, true), 1);
    addEvent(transactionalTable, new IPCappingEvent(((yesterday.plusHours(25).toInstant().toEpochMilli()) &
      ~TIME_MASK) << 24l, "IMPRESSION", requestHeaderInvalid, true), 0);
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
    } else if(value instanceof  byte[]) {
      bytes = (byte[])value;
    } else {
      bytes = null;
    }

    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes);
  }

  private void addEvent(HTable table, IPCappingEvent event, int slice) throws IOException {

    ByteArrayOutputStream stream = new ByteArrayOutputStream(10);
    ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
    short modValue = (short)slice;
    buffer.putShort(modValue);

    try {
      stream.write(buffer.array());
      stream.write(Bytes.toBytes(event.getSnapshotId()));
    } catch (IOException e) {
      System.out.println("Failed to write modulo value to stream");
    }

    //byte[] modValue = Bytes.toBytes(slice);
    byte[] identifier = ByteBuffer.wrap(stream.toByteArray()).array();

    Put put = new Put(identifier);
    putCell(put, TRANSACTION_CF_DEFAULT, "snapshot_id", event.getSnapshotId());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", event.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", event.getRequestHeaders());
    table.put(put);
  }
}