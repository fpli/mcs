package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.BaseSparkJob;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * The IPCappingRule is to check whether there are too many clicks
 * from specific IP address during a certain period.
 */
public class IPCappingRuleJob extends BaseSparkJob {
  private final Logger logger;
  private final String table;
  private final long time;
  private final long timeRange;
  private Configuration hbaseConf;
  private static final long TIME_MASK = 0xFFFFFFl << 40l;

  // Only for test
  private long numberOfRow;

  public IPCappingRuleJob(String jobName, String mode, String table, long time, long timeRange) {
    super(jobName, mode, false);
    this.table = table;
    this.time = time;
    this.timeRange = timeRange;
    this.logger = logger();
  }

  @Override
  public void run() {
    JavaSparkContext sc = javaSparkContext();
    initHBaseConf();

    Scan scan = new Scan();
    // TODO: mark for time
    scan.setStartRow(Bytes.toBytes(time - timeRange));
    scan.setStopRow(Bytes.toBytes(time));

    hbaseConf.set(TableInputFormat.INPUT_TABLE, table);
    try {
      hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
    } catch (IOException e) {
      logger.error(e.getMessage());
      throw new RuntimeException(e);
    }

    JavaPairRDD<ImmutableBytesWritable, Result> rdd =
            sc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

    // For test
    numberOfRow = rdd.count();
  }

  private void initHBaseConf() {
    if (hbaseConf == null) {
      hbaseConf = HBaseConfiguration.create();
    }
  }

  /**
   * Only for test
   */
  public void setHBaseConf(Configuration hbaseConf) {
    this.hbaseConf = hbaseConf;
  }

  /**
   * Only for test
   */
  public long getNumberOfRow() {
    return numberOfRow;
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    Option jobName = new Option((String)null, "jobName", true, "The job name");
    options.addOption(jobName);

    Option mode = new Option((String)null, "mode", true, "spark on yarn or local");
    mode.setRequired(true);
    options.addOption(mode);

    Option table = new Option((String)null, "table", true, "HBase table");
    table.setRequired(true);
    options.addOption(table);

    Option time = new Option((String)null, "time", true, "The time point for IP capping rule");
    time.setRequired(true);
    options.addOption(time);

    Option timeRange = new Option((String)null, "time_range", true, "time range for IP capping rule");
    timeRange.setRequired(true);
    options.addOption(timeRange);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("IPCappingRuleJob", options);

      System.exit(1);
      return;
    }

    IPCappingRuleJob job = new IPCappingRuleJob(cmd.getOptionValue("jobName"),
            cmd.getOptionValue("mode"), cmd.getOptionValue("table"), Long.parseLong(cmd.getOptionValue("time")),
            Long.parseLong(cmd.getOptionValue("timeRange")));
    try {
      job.run();
    } finally {
      job.stop();
    }
  }
}
