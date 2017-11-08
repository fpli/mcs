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
import org.junit.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

public class TestIPCappingRuleJob {
  final String TRANSACTION_TABLE_NAME = "prod_transactional";
  final String TRANSACTION_CF_DEFAULT = "x";

  private HBaseTestingUtility hbaseUtility;
  private Configuration hbaseConf;
  private Connection hbaseConnection;
  private HTable transactionalTable;
  private HBaseAdmin hbaseAdmin;

  @Before
  public void setUp() throws Exception {
    hbaseUtility = new HBaseTestingUtility();
    hbaseUtility.startMiniCluster();

    hbaseConf = hbaseUtility.getConfiguration();
    hbaseConnection = hbaseUtility.getConnection();
    hbaseAdmin = hbaseUtility.getHBaseAdmin();
    transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), hbaseConnection);

    initHBaseTransactionTable();
  }

  @After
  public void tearDown() throws Exception {
    hbaseUtility.shutdownMiniCluster();
  }

  @Test
  public void testIPCappingRuleJob() throws Exception {
    IPCappingRuleJob job = new IPCappingRuleJob("TestIPCappingRuleJob", "local[4]", TRANSACTION_TABLE_NAME, 1007, 5);

    job.setHBaseConf(hbaseConf);
    job.run();
    Assert.assertEquals(5, job.getNumberOfRow());
    job.stop();
  }

  private void initHBaseTransactionTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
            .setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);

    long current = System.currentTimeMillis();

    addEvent(transactionalTable, new Event(1001, current, 0, 2, 100, true, true));
    addEvent(transactionalTable, new Event(1002, current + 10, 0, 3, 101, true, true));
    addEvent(transactionalTable, new Event(1003, current + 10, 0, 3, 101, true, true));
    addEvent(transactionalTable, new Event(1004, current + 10, 0, 3, 101, true, true));
    addEvent(transactionalTable, new Event(1005, current + 10, 0, 3, 101, true, true));
    addEvent(transactionalTable, new Event(1006, current + 10, 0, 4, 102, true, true));
    addEvent(transactionalTable, new Event(1007, current + 10, 0, 5, 103, true, true));
    addEvent(transactionalTable, new Event(1008, current + 100, 0, 6, 104, true, true));
  }

  private <T> void putCell(Put put, String family, String qualifier, T value)  {

    byte[] bytes;
    if(value instanceof Long) {
      bytes = Bytes.toBytes(((Long) value).longValue());
    }
    else if(value instanceof  String) {
      bytes = Bytes.toBytes((String)value);
    }
    else if(value instanceof Boolean) {
      bytes = Bytes.toBytes(((Boolean) value).booleanValue());
    }
    else if(value instanceof Timestamp) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      String str = ((Timestamp) value).toString();
      int bufferSize = str.getBytes().length;
      System.arraycopy(str.getBytes(), 0, buffer.array(), 0, bufferSize);
      bytes = buffer.array();
    }
    else{
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
    putCell(put, TRANSACTION_CF_DEFAULT, "is_tracked", event.getTracked());
    putCell(put, TRANSACTION_CF_DEFAULT, "is_valid", event.getValid());
    table.put(put);
  }
}
