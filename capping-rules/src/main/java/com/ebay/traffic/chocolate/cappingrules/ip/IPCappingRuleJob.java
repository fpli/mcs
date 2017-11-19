package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.BaseSparkJob;
import com.ebay.traffic.chocolate.cappingrules.HBaseConnection;
import com.ebay.traffic.chocolate.cappingrules.HBaseScanIterator;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
  private static short MOD = 293;

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
  static Function<Result, IPCappingEvent> readHBaseMapFunc = new
    Function<Result, IPCappingEvent>() {
      @Override
      public IPCappingEvent call(
        Result entry) throws Exception {
        Result r = entry;
        byte[] keyRowBytes = r.getRow();

        IPCappingEvent event = new IPCappingEvent();
        event.setIdentifier(keyRowBytes);
        event.setRequestHeaders(Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_headers"))));
        event.setChannelAction(Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("channel_action"))));
        byte[] cappingPassed = r.getValue(Bytes.toBytes("x"), Bytes.toBytes("capping_passed"));
        if (cappingPassed != null) {
          event.setCappingPassed(Bytes.toBoolean(cappingPassed));
        }
        event.setCappingFailedRule(Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("capping_failed_rule"))));
        return event;
      }
    };

  /**
   * map function used to read from dataframe to javardd
   */
  static Function<Row, IPCappingEvent> mapFunc = new Function<Row, IPCappingEvent>() {
    @Override
    public IPCappingEvent call(Row row) throws Exception {
      IPCappingEvent event = new IPCappingEvent((byte[]) row.get(row.fieldIndex("identifier")), "IPCappingRule", row
        .getBoolean(row.fieldIndex("cappingPassedTmp")));
      return event;
    }
  };

  /**
   * pairFunction used to map poj to hbase for writing
   */
  static PairFunction<IPCappingEvent, ImmutableBytesWritable, Put> writeHBaseMapFunc = new
    PairFunction<IPCappingEvent, ImmutableBytesWritable, Put>() {
      @Override
      public Tuple2<ImmutableBytesWritable, Put> call(IPCappingEvent event)
        throws Exception {
        Put put = new Put(event.getIdentifier());
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
  static VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>> hbasePutFunc = new
    VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>>() {

      @Override
      public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> tupleIter) throws Exception {

        HTable transactionalTable = new HTable(TableName.valueOf("capping_result"), HBaseConnection.getConnection());
        Tuple2<ImmutableBytesWritable, Put> tuple = null;
        while (tupleIter.hasNext()) {
          tuple = tupleIter.next();
          transactionalTable.put(tuple._2);
        }
        transactionalTable.close();
      }
    };

  /**
   * Generate 10 bytes row key
   *
   * @param timestamp start time
   * @param modValue  slice value
   * @return row key byte array
   */
  public byte[] generateIdentifier(long timestamp, short modValue) {
    byte[] snapshotID = Bytes.toBytes((timestamp & ~TIME_MASK) << 24l);

    ByteArrayOutputStream streamStart = new ByteArrayOutputStream(10);
    ByteBuffer bufferStart = ByteBuffer.allocate(Short.BYTES);
    bufferStart.putShort(modValue);

    try {
      streamStart.write(bufferStart.array());
      streamStart.write(snapshotID);
    } catch (IOException e) {
      logger.error("Failed to write modulo value to stream", e);
    }

    byte[] identifier = ByteBuffer.wrap(streamStart.toByteArray()).array();
    return identifier;
  }

  /**
   * Read events into dataframe
   *
   * @param table hbase table
   * @return dataframe containing the data in the scan range
   */
  public Dataset<Row> readEvents(final String table) {
    List<Integer> slices = new ArrayList<Integer>(MOD);
    for (int i = 0; i < MOD; i++) {
      slices.add(i);
    }

    JavaRDD<Result> javaRDD = jsc().parallelize(slices, slices.size()).mapPartitions(
      new FlatMapFunction<Iterator<Integer>, Result>() {

      @Override
      public HBaseScanIterator call(Iterator<Integer>
             integerIterator) throws Exception {
        Integer slice;
        slice = integerIterator.next();
        Configuration hbaseConf = HBaseConnection.getConfiguration();
        try {
          HBaseAdmin.checkHBaseAvailable(hbaseConf);
          logger.info("HBase is " + "running!");
        } catch
          (MasterNotRunningException e) {
          logger.error("HBase is " + "not running!");
          logger.error(e.getMessage());
          throw new MasterNotRunningException(e);
        } catch (Exception ce) {
          logger.error("Unexpected" + " exception when check" + " HBase!");
          logger.error(ce.getMessage());
          throw new Exception(ce);
        }

        long timestampStart = time - timeRange;
        long timestampStop = time;

        byte[] rowkeyStartBytes = generateIdentifier(timestampStart, slice.shortValue());
        byte[] rowkeyStopBytes = generateIdentifier(timestampStop, slice.shortValue());

        HBaseScanIterator hBaseScanIterator = new HBaseScanIterator(table, rowkeyStartBytes, rowkeyStopBytes);
        return hBaseScanIterator;
      }
    });

    JavaRDD<IPCappingEvent> rowRDD = javaRDD.map(readHBaseMapFunc);
    Dataset<Row> schemaRDD = sqlsc().createDataFrame(rowRDD, IPCappingEvent.class);
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
    Dataset<Row> schemaRDDWithIP = schemaRDDClick.withColumn("headerWithIP", functions.split(schemaRDDClick.col
      ("requestHeaders"), "X-eBay-Client-IP:").getItem(1));
    Dataset<Row> schemaRDDWithIPOnly = schemaRDDWithIP.withColumn("headerWithIPOnly", functions.split(schemaRDDWithIP
      .col("headerWithIP"), "\\|").getItem(0));
    Dataset<Row> schemaRDDWithIPTrim = schemaRDDWithIPOnly.withColumn("ip", functions.trim(schemaRDDWithIPOnly.col
      ("headerWithIPOnly")))
      .drop("headerWithIP").drop("headerWithIPOnly");

    WindowSpec window = Window.partitionBy(schemaRDDWithIPTrim.col("ip")).orderBy(schemaRDDWithIPTrim.col("ip"));
    Dataset<Row> df = schemaRDDWithIPTrim.withColumn("count", functions.count("*").over(window));
    Dataset<Row> res = df.withColumn("cappingPassedTmp", functions.when(df.col("count").$greater(threshold), false)
      .otherwise(true));
    Dataset<Row> invalids = res.filter(res.col("cappingPassedTmp").equalTo(false));
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
   * Only for test
   */
  public static void setMod(short mod) {
    MOD = mod;
  }

  public static CommandLine parseOptions(String[] args) {
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
      return null;
    }
    return cmd;
  }


  public static void main(String[] args) {

    CommandLine cmd = parseOptions(args);

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
