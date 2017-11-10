package com.ebay.traffic.chocolate.cappingrules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;

public class TestSNIDCappingRule {
  final String TRANSACTION_TABLE_NAME = "prod_transactional";
  final String TRANSACTION_CF_DEFAULT = "x";
  final String CAPPINGRESULT_TABLE_NAME = "capping_result";

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
    initHBaseCappingResultTable();
  }
  
  private void initHBaseCappingResultTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(CAPPINGRESULT_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT).setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);
  }

  @After
  public void tearDown() throws Exception {
    hbaseUtility.shutdownMiniCluster();
  }

  @Test
  public void testSNIDCappingRuleJob() throws Exception {
    SNIDCappingRule job = new SNIDCappingRule("TestSNIDCappingRule", "local[4]", TRANSACTION_TABLE_NAME, new Date()
        .getTime(),
        1000*60*60*24*4);
    job.setHBaseConf(hbaseConf);
    job.run();
    JavaPairRDD<Long, Event> result = job.readEvents(TRANSACTION_TABLE_NAME);
    Assert.assertEquals(12, result.count());
    //Dataset<Row> modifiedResult = result.filter(result.col("cappingPassed").equalTo(false));
    Assert.assertEquals(4, job.getNumberOfRow());
    job.stop();
  }

  private void initHBaseTransactionTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
            .setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);

    addEvent(transactionalTable, new Event(6889640599780065280l, "CLICK", 100));
    addEvent(transactionalTable, new Event(6889640599780081664l, "IMPRESSION", 100));
    addEvent(transactionalTable, new Event(6889640599780098048l, "CLICK", 200));
    addEvent(transactionalTable, new Event(6889640599780114432l, "IMPRESSION", 200));
    addEvent(transactionalTable, new Event(6889640599780130816l, "CLICK", 300));
    addEvent(transactionalTable, new Event(6889640599780147200l, "CLICK", 300));
    addEvent(transactionalTable, new Event(6889640599780163584l, "CLICK", 300));
    addEvent(transactionalTable, new Event(6889640599780179968l, "IMPRESSION", 300));
    addEvent(transactionalTable, new Event(6889640599780196352l, "CLICK", 400));
    addEvent(transactionalTable, new Event(6889640599780212736l, "CLICK", 400));
    addEvent(transactionalTable, new Event(6889640599780229120l, "CLICK", 400));
    addEvent(transactionalTable, new Event(6889640599780245504l, "CLICK", 400));
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
    putCell(put, TRANSACTION_CF_DEFAULT, "snid", event.getSnid());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", event.getChannelAction());
    table.put(put);
  }
}
