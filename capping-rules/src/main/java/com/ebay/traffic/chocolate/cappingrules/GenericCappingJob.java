package com.ebay.traffic.chocolate.cappingrules;

import com.google.protobuf.ServiceException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by yimeng on 11/12/17.
 */
public class GenericCappingJob {
  // format timestamp to byte
  protected static final long TIME_MASK = 0xFFFFFFl << 40l;
  private final Logger logger = LoggerFactory.getLogger(GenericCappingJob.class);
  // environment context
  protected Configuration hbaseConf;
  protected JavaSparkContext javaSparkContext;
  
  
  public GenericCappingJob(Configuration hbaseConf, JavaSparkContext javaSparkContext) {
    this.hbaseConf = hbaseConf;
    this.javaSparkContext = javaSparkContext;
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
        javaSparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
    
    return hBaseRDD;
  }
  
  
  public void writeToHbase(JavaPairRDD<Long, Event> filterResult, String table, PairFunction<Event,
      ImmutableBytesWritable,
      Put> writeHBaseMapFunc) {
    
    Job newAPIJobConfiguration = null;
    try {
      newAPIJobConfiguration = Job.getInstance(hbaseConf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
    newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
    
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = filterResult.values().mapToPair(writeHBaseMapFunc);
    hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
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
}
