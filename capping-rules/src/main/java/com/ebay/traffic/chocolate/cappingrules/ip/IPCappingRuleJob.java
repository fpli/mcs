package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.BaseSparkJob;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import scala.Tuple2;


import java.io.IOException;
import java.util.Iterator;

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
  private static final long TIME_MASK = 0xFFFFFFl << 40l;
  private static Configuration hbaseConfigForTest;

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
  static PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, IPCappingEvent> readHBaseMapFunc = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, IPCappingEvent>() {
    @Override
    public Tuple2<Long, IPCappingEvent> call(
      Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

      Result r = entry._2;
      long keyRow = Bytes.toLong(r.getRow());

      IPCappingEvent event = new IPCappingEvent();
      event.setSnapshotId(keyRow);
      event.setRequestHeaders((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_headers"))));
      event.setChannelAction((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("channel_action"))));
      byte[] cappingPassed = r.getValue(Bytes.toBytes("x"), Bytes.toBytes("capping_passed"));
      if(cappingPassed!=null) {
        event.setCappingPassed((Boolean) Bytes.toBoolean(cappingPassed));
      }
      return new Tuple2<Long, IPCappingEvent>(keyRow, event);
    }
  };

  /**
   * map function used to read from dataframe to javardd
   */
  static Function<Row, IPCappingEvent> mapFunc = new Function<Row, IPCappingEvent>() {
    @Override
    public IPCappingEvent call(Row row) throws Exception {
      IPCappingEvent event = new IPCappingEvent(row.getLong(row.fieldIndex("snapshotId")), "IPCappingRule", row.getBoolean(row.fieldIndex("cappingPassedTmp")));
      return event;
    }
  };

  /**
   * pairFunction used to map poj to hbase for writing
   */
  static PairFunction<IPCappingEvent, ImmutableBytesWritable, Put> writeHBaseMapFunc = new PairFunction<IPCappingEvent, ImmutableBytesWritable, Put>() {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(IPCappingEvent event)
      throws Exception {
      Put put = new Put(Bytes.toBytes(event.getSnapshotId()));
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes("capping_passed"),
        Bytes.toBytes(false));
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes("capping_failed_rule"),
        Bytes.toBytes("IPCappingRule"));
      return new Tuple2<ImmutableBytesWritable, Put>(
        new ImmutableBytesWritable(), put);
    }
  };

  /**
   * Function used to write hbase
   */
  static VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>> hbasePutFunc = new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>>() {

    @Override
    public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> tupleIter) throws Exception {

      HTable transactionalTable = new HTable(TableName.valueOf("capping_result"), ConnectionFactory.createConnection(getHBaseConf()));
      Tuple2<ImmutableBytesWritable, Put> tuple = null;
      while(tupleIter.hasNext()) {
        tuple = tupleIter.next();
        transactionalTable.put(tuple._2);
      }
      transactionalTable.close();
    }
  };

  /**
   * Read events into dataframe
   *
   * @param table hbase table
   * @return dataframe containing the data in the scan range
   */
  public Dataset<Row> readEvents(String table) throws Exception {
    Configuration hbaseConf = getHBaseConf();
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

    JavaPairRDD<Long, IPCappingEvent> rowPairRDD = hBaseRDD.mapToPair(readHBaseMapFunc);
    Dataset<Row> schemaRDD = sqlsc().createDataFrame(rowPairRDD.values(), IPCappingEvent.class);
    schemaRDD.show();
    return schemaRDD;
  }

  /**
   * Filter ipcapping events
   *
   * @param table hbase table
   * @return invalids events which don't pass the capping rule
   */
  public Dataset<Row> filterEvents(String table) throws Exception {

    Dataset<Row> schemaRDD = readEvents(table);
    Dataset<Row> schemaRDDClick = schemaRDD.filter(schemaRDD.col("channelAction").equalTo("CLICK"));
    Dataset<Row> schemaRDDWithIP = schemaRDDClick.withColumn("headerWithIP", functions.split(schemaRDDClick.col("requestHeaders"), "X-eBay-Client-IP:").getItem(1));
    Dataset<Row> schemaRDDWithIPOnly = schemaRDDWithIP.withColumn("headerWithIPOnly", functions.split(schemaRDDWithIP.col("headerWithIP"), "\\|").getItem(0));
    Dataset<Row> schemaRDDWithIPTrim = schemaRDDWithIPOnly.withColumn("ip", functions.trim(schemaRDDWithIPOnly.col("headerWithIPOnly")))
      .drop("headerWithIP").drop("headerWithIPOnly");

    WindowSpec window = Window.partitionBy(schemaRDDWithIPTrim.col("ip")).orderBy(schemaRDDWithIPTrim.col("ip"));
    Dataset<Row> df = schemaRDDWithIPTrim.withColumn("count", functions.count("*").over(window));
    Dataset<Row> res = df.withColumn("cappingPassedTmp", functions.when(df.col("count").$greater(threshold), false).otherwise(true));
    Dataset<Row> invalids = res.filter(res.col("cappingPassedTmp").equalTo(false));
    invalids.show();
    return invalids;
  }


  /**
   * write invalids events back to hbase
   *
   * @param invalids invalids events
   */
  public void writeInvalidEvents(Dataset<Row> invalids) throws IOException {

    JavaRDD<IPCappingEvent> records = invalids.toJavaRDD().map(mapFunc);
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(writeHBaseMapFunc);
    hbasePuts.foreachPartition(hbasePutFunc);
  }

  /**
   * Filter rule entrance
   */
  @Override
  public void run() throws Exception {
    Dataset<Row> failedRecords = filterEvents(this.table);
    writeInvalidEvents(failedRecords);
  }

  /**
   * When in unit test, the configuration is set by setHBaseConf. In production, we create from environment.
   * @return hbase configuration
   */
  private static Configuration getHBaseConf() {
    if(hbaseConfigForTest==null){
      return HBaseConfiguration.create();
    }
    return hbaseConfigForTest;
  }

  /**
   * Only for test
   */
  public void setHBaseConf(Configuration hbaseConf) {
    this.hbaseConfigForTest = hbaseConf;
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
    try {
      job.run();
    } catch (Exception ex) {
      System.exit(1);
    } finally {
      job.stop();
    }
  }
}
