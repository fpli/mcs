package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.BaseSparkJob;
import com.ebay.traffic.chocolate.cappingrules.dto.CapperIdentity;
import com.google.protobuf.ServiceException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * Created by yimeng on 11/12/17.
 */
public abstract class AbstractCapper extends BaseSparkJob {
  // format timestamp to byte
  protected static final long TIME_MASK = 0xFFFFFFl << 40l;
  protected static final long HIGH_24 = 0x15000000000l;
  protected static final byte[] columnFamily = Bytes.toBytes("x");
  protected static final String INPUT_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
  protected static String resultTable = "snid_capping_result";
  protected static Configuration hbaseConf;
  protected static VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>> hbasePutFunc = new
      VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>>() {
        
        @Override
        public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> tupleIter) throws Exception {
          
          HTable transactionalTable = new HTable(TableName.valueOf(resultTable), ConnectionFactory.createConnection());
          Tuple2<ImmutableBytesWritable, Put> tuple = null;
          while (tupleIter.hasNext()) {
            tuple = tupleIter.next();
            transactionalTable.put(tuple._2);
          }
          transactionalTable.close();
        }
      };
  protected final String originalTable;
  protected final long startTimestamp;
  protected final long endTimestamp;
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  
  public AbstractCapper(String jobName, String mode, String originalTable, String resultTable, String startTime,
                        String endTime) throws ParseException {
    super(jobName, mode, false);
    this.originalTable = originalTable;
    this.resultTable = resultTable;
    this.startTimestamp = new SimpleDateFormat(INPUT_DATE_FORMAT).parse(startTime).getTime() / 1000;
    this.endTimestamp = new SimpleDateFormat(INPUT_DATE_FORMAT).parse(endTime).getTime() / 1000;
  }
  
  public static Options getJobOptions(String cappingRuleDescription) {
    Options options = new Options();
    Option jobName = new Option((String) null, "jobName", true, "The job name");
    options.addOption(jobName);
    
    Option mode = new Option((String) null, "mode", true, "spark on yarn or local");
    mode.setRequired(true);
    options.addOption(mode);
    
    Option originalTable = new Option((String) null, "originalTable", true, "originalTable read from HBase");
    originalTable.setRequired(true);
    options.addOption(originalTable);
    
    Option resultTable = new Option((String) null, "resultTable", true, "resultTable write to HBase");
    resultTable.setRequired(true);
    options.addOption(resultTable);
    
    Option startTime = new Option((String) null, "startTime", true, "the startTime for " + cappingRuleDescription);
    startTime.setRequired(true);
    options.addOption(startTime);
    
    Option endTime = new Option((String) null, "endTime", true, "the endTime for " + cappingRuleDescription);
    endTime.setRequired(true);
    options.addOption(endTime);
    
    return options;
  }
  
  public static long getTimeMillis(long snapshotId) {
    return snapshotId >>> 24l | HIGH_24;
  }
  
  /**
   * Initialize hbase config
   */
  protected static Configuration getHBaseConf() {
    if (hbaseConf == null) {
      hbaseConf = HBaseConfiguration.create();
    }
    return hbaseConf;
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
    
    logger.info("originalTable = " + table);
    logger.info("startTimestamp = " + startTimestamp);
    logger.info("stopTimestamp = " + stopTimestamp);
    
    HBaseAdmin.checkHBaseAvailable(hbaseConf);
    hbaseConf.set(TableInputFormat.INPUT_TABLE, table);
    
    
    
    byte[] startRow = Bytes.toBytes((startTimestamp & ~TIME_MASK) << 24l);
    byte[] stopRow = Bytes.toBytes((stopTimestamp & ~TIME_MASK) << 24l);
    
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
}
