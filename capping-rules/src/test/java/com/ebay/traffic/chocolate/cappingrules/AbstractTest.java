package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.cappingrules.Rules.SNIDCapper;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperResult;
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
import org.apache.spark.api.java.function.PairFunction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;

/**
 * Created by yimeng on 11/15/17.
 */
public abstract class AbstractTest {
  /**
   * Maximum driver ID constant.
   */
  public static final long MAX_DRIVER_ID = 0x3FFl;
  protected final static byte[] columnX = Bytes.toBytes("x");
  // Mask for the high 24 bits in a timestamp
  protected static final long TIME_MASK = 0xFFFFFFl << 40l;
  static PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, SNIDCapperResult> readHBaseResultMapFunc = new
      PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, SNIDCapperResult>() {
        @Override
        public Tuple2<Long, SNIDCapperResult> call(
            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

          Result r = entry._2;
          long keyrow = Bytes.toLong(r.getRow());

          SNIDCapperResult snidResult = new SNIDCapperResult();
          snidResult.setSnapshotId(keyrow);
          snidResult.setImpressed(Bytes.toBoolean(r.getValue(columnX, Bytes.toBytes("is_impressed"))));
          //snidResult.setImpSnapshotId(Bytes.toLong(r.getValue(columnX, Bytes.toBytes("imp_snapshot_id"))));
          return new Tuple2<Long, SNIDCapperResult>(keyrow, snidResult);
        }
      };
  protected final String TRANSACTION_TABLE_NAME = "prod_transactional";
  protected final String TRANSACTION_CF_DEFAULT = "x";
  protected final String CAPPINGRESULT_TABLE_NAME = "snid_capping_result";
  protected HBaseTestingUtility hbaseUtility;
  protected Configuration hbaseConf;
  protected Connection hbaseConnection;
  protected HTable transactionalTable;
  protected HBaseAdmin hbaseAdmin;

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
    tableDesc.addFamily(new HColumnDescriptor(CAPPINGRESULT_TABLE_NAME).setCompressionType(Compression.Algorithm.NONE));
    hbaseAdmin.createTable(tableDesc);
  }
  
  protected abstract void initHBaseTransactionTable() throws IOException;

  @After
  public void tearDown() throws Exception {
    hbaseUtility.shutdownMiniCluster();
  }

  @Test
  public void testSNIDCappingRuleJob() throws Exception {
    Calendar c = Calendar.getInstance();
    c.add(Calendar.HOUR, 1);
    long time = c.getTimeInMillis();
    long timeRange = 1000 * 60 * 60 * 24 * 4;
    SNIDCapper job = new SNIDCapper("TestSNIDCappingRule", "local[4]", TRANSACTION_TABLE_NAME,
        CAPPINGRESULT_TABLE_NAME, "2017-11-14 00:00:00", "2017-11-15 23:59:59");
    job.setHbaseConf(hbaseConf);
    job.run();

    //validate original test data
    //GenericCappingJob genericCappingJob = new GenericCappingJob(hbaseConf, job.jsc());
    long startTimestamp = time - timeRange;
    JavaPairRDD<ImmutableBytesWritable, Result> testData = job.readFromHabse(TRANSACTION_TABLE_NAME,
        startTimestamp, time);
    Assert.assertEquals(14, testData.count());
    //validate result data
    JavaPairRDD<ImmutableBytesWritable, Result> resultData = job.readFromHabse(CAPPINGRESULT_TABLE_NAME,
        startTimestamp, time);
    Assert.assertEquals(7, resultData.count());
    JavaPairRDD<Long, SNIDCapperResult> resultMapRDD = resultData.mapToPair(readHBaseResultMapFunc);
    Iterator<SNIDCapperResult> eventIterator = resultMapRDD.values().toLocalIterator();
    while (eventIterator.hasNext()) {
      SNIDCapperResult e = eventIterator.next();
      Assert.assertEquals(false, e.getImpressed());
      //Assert.assertNotEquals(java.util.Optional.of(0), e.getImpSnapshotId());
    }
    job.stop();
  }

  private long getSnapshotId(long epochMilliseconds, long driverId) {
    Validate.isTrue(driverId >= 0 && driverId <= MAX_DRIVER_ID);
    return ((epochMilliseconds & ~TIME_MASK) << 24l) | (driverId << 14l);
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
}
