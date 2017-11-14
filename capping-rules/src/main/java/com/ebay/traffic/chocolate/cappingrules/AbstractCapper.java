package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.BaseSparkJob;
import com.ebay.traffic.chocolate.cappingrules.dto.CapperIdentity;
import com.google.protobuf.ServiceException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by yimeng on 11/12/17.
 */
public abstract class AbstractCapper extends BaseSparkJob {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  // format timestamp to byte
  protected static final long TIME_MASK = 0xFFFFFFl << 40l;
  protected static final long HIGH_24 = 0x15000000000l;
  protected static final byte[] columnFamily = Bytes.toBytes("x");
  protected static final String INPUT_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
  protected final String originalTable;
  protected final String resultTable;
  protected final long startTimestamp;
  protected final long endTimestamp;
  protected Configuration hbaseConf;
  
  public AbstractCapper(String jobName, String mode, String originalTable, String resultTable, String startTime,
                           String endTime) throws ParseException {
    super(jobName, mode, false);
    this.originalTable = originalTable;
    this.resultTable = resultTable;
    this.startTimestamp = new SimpleDateFormat(INPUT_DATE_FORMAT).parse(startTime).getTime();
    this.endTimestamp = new SimpleDateFormat(INPUT_DATE_FORMAT).parse(endTime).getTime();
  }
  
  public static Options getJobOptions(String cappingRuleDescription) {
    Options options = new Options();
    Option jobName = new Option((String) null, "jobName", true, "The job name");
    options.addOption(jobName);
    
    Option mode = new Option((String) null, "mode", true, "spark on yarn or local");
    mode.setRequired(true);
    options.addOption(mode);
    
    Option table = new Option((String) null, "table", true, "HBase table");
    table.setRequired(true);
    options.addOption(table);
    
    Option time = new Option((String) null, "time", true, "The time point for " + cappingRuleDescription);
    time.setRequired(true);
    options.addOption(time);
    
    Option timeRange = new Option((String) null, "time_range", true, "time range for " + cappingRuleDescription);
    timeRange.setRequired(true);
    options.addOption(timeRange);
    
    return options;
  }
  
  public void setHbaseConf(Configuration hbaseConf) {
    this.hbaseConf = hbaseConf;
  }
  
  /**
   * run capping job
   */
  @Override
  public void run() throws Exception {
    
    JavaPairRDD<ImmutableBytesWritable, Result> hbaseData = readFromHabse(originalTable,
        startTimestamp, endTimestamp);
    
    JavaPairRDD<Long, CapperIdentity> filterResult = filterWithCapper(hbaseData);
    
    writeToHbase(filterResult, resultTable);
  }
  
  
  /**
   * Read Data with data range from hbase
   *
   * @param table          HBase table
   * @param startTimestamp scan row start timestamp
   * @param stopTimestamp  scan row stop timestamp
   * @return hbase scan result
   */
  protected JavaPairRDD<ImmutableBytesWritable, Result> readFromHabse(String table, long startTimestamp, long
      stopTimestamp) throws IOException, ServiceException {
    HBaseAdmin.checkHBaseAvailable(hbaseConf);
    hbaseConf.set(TableInputFormat.INPUT_TABLE, table);
    
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
    byte[] startRow = buffer.putLong((startTimestamp & ~TIME_MASK) << 24l).array();
    buffer = ByteBuffer.allocate(Long.SIZE);
    byte[] stopRow = buffer.putLong((stopTimestamp & ~TIME_MASK) << 24l).array();
    
    Scan scan = new Scan();
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    
    try {
      hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
        jsc().newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
    
    return hBaseRDD;
  }
  
  protected abstract <T> T filterWithCapper(JavaPairRDD<ImmutableBytesWritable, Result> hbaseData);
  
  public abstract <T> void writeToHbase(T writeData, String table);
  
  public static long getTimeMillis(long snapshotId) {
    return snapshotId >>> 24l | HIGH_24;
  }
  
}
