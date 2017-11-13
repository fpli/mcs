package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.BaseSparkJob;
import com.ebay.traffic.chocolate.cappingrules.Event;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.IOException;

/**
 * The IPCappingRule is to check whether there are too many clicks
 * from specific IP address during a certain period.
 *
 * @author xiangli4
 */
public class IPCappingRuleJob extends BaseSparkJob {
  private final Logger logger;
  private final String table;
  private final long time;
  private final long timeRange;
  private final long threshold;
  private Configuration hbaseConf;
  private static final long TIME_MASK = 0xFFFFFFl << 40l;

  // Only for test
  private long numberOfRow;

  /**
   * Conctructor of the rule
   *
   * @param jobName   spark job name
   * @param mode      spark sumbit mode
   * @param table     hbase table we query from
   * @param time      scan stop time
   * @param timeRange time range from start time to end time
   * @param threshold ip occurance threshold
   */
  public IPCappingRuleJob(String jobName, String mode, String table, long time, long timeRange, long threshold) {
    super(jobName, mode, false);
    this.table = table;
    this.time = time;
    this.timeRange = timeRange;
    this.threshold = threshold;
    this.logger = logger();
  }

  /**
   * PairFunction used to map hbase to event poj
   */
  static PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, Event> readHBaseMapFunc = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, Event>() {
    @Override
    public Tuple2<Long, Event> call(
      Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

      Result r = entry._2;
      long keyRow = Bytes.toLong(r.getRow());

      Event event = new Event();
      event.setSnapshotId(keyRow);
      event.setRequestHeaders((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_headers"))));
      event.setChannelAction((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("channel_action"))));
      event.setCappingFailedRule((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("capping_failed_rule"))));
      event.setCappingPassed((Boolean) Bytes.toBoolean(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("capping_passed"))));
      return new Tuple2<Long, Event>(keyRow, event);
    }
  };

  /**
   * map function used to read from dataframe to javardd
   */
  static Function<Row, Event> mapFunc = new Function<Row, Event>() {
    @Override
    public Event call(Row row) throws Exception {
      Event event = new Event(row.getLong(row.fieldIndex("snapshotId")), "IPCappingRule", row.getBoolean(row.fieldIndex("cappingPassed")));
      return event;
    }
  };

  /**
   * pairFunction used to map poj to hbase for writing
   */
  static PairFunction<Event, ImmutableBytesWritable, Put> writeHBaseMapFunc = new PairFunction<Event, ImmutableBytesWritable, Put>() {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Event event)
      throws Exception {
      Put put = new Put(Bytes.toBytes(event.getSnapshotId()));
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes("capping_passed"),
        Bytes.toBytes(event.isCappingPassed()));
      //TODO:remove put channel_action after testing on testing table
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes("channel_action"),
        Bytes.toBytes("CLICK"));
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes("capping_failed_rule"),
        Bytes.toBytes(event.getCappingFailedRule()));

      return new Tuple2<ImmutableBytesWritable, Put>(
        new ImmutableBytesWritable(), put);
    }
  };

  /**
   * Read events into dataframe
   *
   * @param table hbase table
   * @return dataframe containing the data in the scan range
   */
  public Dataset<Row> readEvents(String table) throws Exception {
    try {
      HBaseAdmin.checkHBaseAvailable(hbaseConf);
      logger.info("HBase is running!");
    } catch (MasterNotRunningException e) {
      logger.error("HBase is not running!");
      logger.error(e.getMessage());
      throw new MasterNotRunningException(e);
    } catch (Exception ce) {
      logger.error("Unexpected exception when check HBase!");
      logger.error(ce.getMessage());
      throw new Exception(ce);
    }

    hbaseConf.set(TableInputFormat.INPUT_TABLE, table);

    long timestampStart = time - timeRange;
    long timestampStop = time;
    byte[] timestampStartBytes = Bytes.toBytes((timestampStart & ~TIME_MASK) << 24l);
    byte[] timestampStopBytes = Bytes.toBytes((timestampStop & ~TIME_MASK) << 24l);

    Scan scan = new Scan();
    scan.setStartRow(timestampStartBytes);
    scan.setStopRow(timestampStopBytes);

    try {
      hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
    } catch (IOException e) {
      logger.error(e.toString());
    }

    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
      jsc().newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

    JavaPairRDD<Long, Event> rowPairRDD = hBaseRDD.mapToPair(readHBaseMapFunc);
    Dataset<Row> schemaRDD = sqlsc().createDataFrame(rowPairRDD.values(), Event.class);
    schemaRDD.show();
    return schemaRDD.filter(schemaRDD.col("channelAction").equalTo("CLICK"));
  }

  /**
   * Filter ipcapping events
   *
   * @param table hbase table
   * @return invalids events which don't pass the capping rule
   */
  public Dataset<Row> filterEvents(String table) throws Exception {

    Dataset<Row> schemaRDD = readEvents(table);
    Dataset<Row> schemaRDDWithIP = schemaRDD.withColumn("headerWithIP", functions.split(schemaRDD.col("requestHeaders"), "X-eBay-Client-IP:").getItem(1));
    Dataset<Row> schemaRDDWithIPOnly = schemaRDDWithIP.withColumn("headerWithIPOnly", functions.split(schemaRDDWithIP.col("headerWithIP"), "\\|").getItem(0));
    Dataset<Row> schemaRDDWithIPTrim = schemaRDDWithIPOnly.withColumn("ip", functions.trim(schemaRDDWithIPOnly.col("headerWithIPOnly")))
      .drop("headerWithIP").drop("headerWithIPOnly");

    WindowSpec window = Window.partitionBy(schemaRDDWithIPTrim.col("ip")).orderBy(schemaRDDWithIPTrim.col("ip"));
    Dataset<Row> df = schemaRDDWithIPTrim.withColumn("count", functions.count("*").over(window));
    Dataset<Row> res = df.withColumn("cappingPassed", functions.when(df.col("count").$greater(threshold), false).otherwise(true));
    Dataset<Row> invalids = res.filter(res.col("cappingPassed").equalTo(false));
    return invalids;
  }

  /**
   * write invalids events back to hbase
   *
   * @param invalids invalids events
   * @param table    hbase table writing to
   */
  public void writeInvalidEvents(Dataset<Row> invalids, String table) {

    Job newAPIJobConfiguration = null;
    try {
      newAPIJobConfiguration = Job.getInstance(hbaseConf);
    } catch (IOException e) {
      logger.error(e.toString());
    }
    newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
    newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

    JavaRDD<Event> records = invalids.toJavaRDD().map(mapFunc);

    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(writeHBaseMapFunc);
    hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
  }

  /**
   * Filter rule entrance
   */
  @Override
  public void run() throws Exception {
    Dataset<Row> failedRecords = filterEvents(this.table);
    //TODO: change it back to prod_transacational table
    writeInvalidEvents(failedRecords, "capping_result");
    // For test
    numberOfRow = failedRecords.count();
  }

  /**
   * Initialize hbase config
   */
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

  public static void main(String[] args) {
    Options options = new Options();
    Option jobName = new Option((String) null, "jobName", true, "The job name");
    options.addOption(jobName);

    Option mode = new Option((String) null, "mode", true, "spark on yarn or local");
    mode.setRequired(true);
    options.addOption(mode);

    Option table = new Option((String) null, "table", true, "HBase table");
    table.setRequired(true);
    options.addOption(table);

    Option time = new Option((String) null, "time", true, "The time point for IP capping rule");
    time.setRequired(true);
    options.addOption(time);

    Option timeRange = new Option((String) null, "timeRange", true, "time range for IP capping rule");
    timeRange.setRequired(true);
    options.addOption(timeRange);

    Option threshold = new Option((String) null, "threshold", true, "threshold for IP capping rule");
    threshold.setRequired(true);
    options.addOption(threshold);

    CommandLineParser parser = new BasicParser();
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
      Long.parseLong(cmd.getOptionValue("timeRange")), Long.parseLong(cmd.getOptionValue("threshold")));
    //TODO: find a better way to get the configuration
    job.initHBaseConf();
    try {
      job.run();
    } catch (Exception ex) {
      System.exit(1);
    } finally {
      job.stop();
    }
  }
}
