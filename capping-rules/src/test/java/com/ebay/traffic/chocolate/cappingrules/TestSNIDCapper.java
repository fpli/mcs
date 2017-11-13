package com.ebay.traffic.chocolate.cappingrules;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import java.util.Calendar;
import java.util.Iterator;

public class TestSNIDCapper {
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
    Calendar c = Calendar.getInstance();
    c.add(Calendar.HOUR, 1);
    long time = c.getTimeInMillis();
    long timeRange = 1000*60*60*24*4;
    SNIDCapper job = new SNIDCapper("TestSNIDCappingRule", "local[4]", TRANSACTION_TABLE_NAME,time,timeRange);
    job.setHBaseConf(hbaseConf);
    job.run();
    GenericCappingJob genericCappingJob = new GenericCappingJob(hbaseConf, job.jsc());
    long startTimestamp = time - timeRange;
    JavaPairRDD<ImmutableBytesWritable, Result> hbaseData = genericCappingJob.readFromHabse(TRANSACTION_TABLE_NAME,
        startTimestamp, time);
    Assert.assertEquals(14, hbaseData.count());
    Iterator<Event> eventIterator = job.getFilteredResult().values().toLocalIterator();
    while(eventIterator.hasNext()){
      Event e = eventIterator.next();
      Assert.assertEquals(false, e.getImpressed());
    }
    Assert.assertEquals(7, job.getNumberOfRow());
    job.stop();
  }

  private void initHBaseTransactionTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
            .setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);
    Calendar c = Calendar.getInstance();
    c.add(Calendar.HOUR, -5);
    c.add(Calendar.MINUTE, 10);
    // click happens after impression on same host and different host
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),201), "IMPRESSION", 200));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),201), "CLICK", 200));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),202), "CLICK", 200));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),202), "CLICK", 200));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),203), "CLICK", 200));
    // click&impression happens at same time
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(), 100), "CLICK", 100));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(), 101), "IMPRESSION", 100));
    // click happens before impression
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(), 300), "CLICK", 400));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(), 301), "CLICK", 400));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(), 302), "CLICK", 400));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(), 303), "IMPRESSION", 400));
    // only click no impression
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),201), "CLICK", 300));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),202), "CLICK", 300));
    addEvent(transactionalTable, new Event(getSnapshotId(c.getTimeInMillis(),203), "CLICK", 300));
  }
  
  // Mask for the high 24 bits in a timestamp
  private static final long TIME_MASK = 0xFFFFFFl << 40l;
  /** Maximum driver ID constant. */
  public static final long MAX_DRIVER_ID = 0x3FFl;
  // Mask for the driver ID
  private static final long DRIVER_MASK = MAX_DRIVER_ID << 14l;
  private long getSnapshotId(long epochMilliseconds, long driverId){
    Validate.isTrue(driverId >= 0 && driverId <= MAX_DRIVER_ID);
    return ((epochMilliseconds & ~TIME_MASK) << 24l) | (driverId << 14l);
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
