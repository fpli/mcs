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
import org.apache.spark.api.java.function.*;
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
  // slf4j logger
  private final Logger logger;
  // Rule name. It will be written to HBase if one record does not pass.
  private final String RULE_NAME = "IPCappingRule";
  // Capping passed column name.
  private final String CAPPING_PASSED_COL = "capping_passed";
  // Capping failed rule column name.
  private final String CAPPING_FAILED_RULE_COL = "capping_failed_rule";
  // Input table name. Read records from this table.
  private final String table;
  // Result table name. In production we have it the same as input table.
  private final String resultTable;
  // The scan stop time. In each job, it should be the current timestamp in millisecond.
  private final long time;
  // The scan range. If it is a day, it will be 24*60*60*1000.
  private final long timeWindow;
  // Write HBase time window start time. The value is get from time - timeWindow.
  private final long updateWindowStartTime;
  // IP count threshold. Used to judge is an IP is invalid.
  private final long threshold;
  // Mask for the high 24 bits in a timestamp.
  private static final long TIME_MASK = 0xFFFFFFl << 40l;
  // The high 26f bits of system time.
  private static final long HIGH_24 = 0x10000000000l;
  // Constant representing the default modulus for modulus-based row identifiers.
  private static short MOD = 293;

  /**
   * Conctructor of the rule
   *
   * @param jobName   spark job name
   * @param mode      spark sumbit mode
   * @param table     hbase table we query from
   * @param resultTable result table
   * @param time      scan stop time
   * @param timeWindow scan time window from start time to end time
   * @param updateTimeWindow update time window for HBase
   * @param threshold ip occurance threshold
   */
  public IPCappingRuleJob(String jobName, String mode, String table, String resultTable, long time, long timeWindow, long updateTimeWindow, long threshold) {
    super(jobName, mode, false);
    this.table = table;
    this.resultTable = resultTable;
    this.time = time;
    this.timeWindow = updateTimeWindow;
    this.updateWindowStartTime = time - timeWindow;
    this.threshold = threshold;
    this.logger = logger();
  }

  /**
   * Main function. Get parameters and then run the job.
   * @param args
   */
  public static void main(String[] args) {
    CommandLine cmd = parseOptions(args);
    IPCappingRuleJob job = new IPCappingRuleJob(cmd.getOptionValue("jobName"),
      cmd.getOptionValue("mode"), cmd.getOptionValue("table"), cmd.getOptionValue("resultTable"), Long.parseLong(cmd.getOptionValue("time")),
      Long.parseLong(cmd.getOptionValue("timeWindow")), Long.parseLong(cmd.getOptionValue("updateTimeWindow")), Long.parseLong(cmd.getOptionValue("threshold")));
    try {
      job.run();
    } catch (Exception ex) {
      System.exit(1);
    } finally {
      job.stop();
    }
  }

  /**
   * Parse command line arguments
   * @param args arguments
   * @return CommandLine used to get every argument
   */
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

    Option resultTable = new Option((String) null, "resultTable", true, "HBase result table");
    resultTable.setRequired(true);
    options.addOption(resultTable);

    Option time = new Option((String) null, "time", true, "The time point for IP capping rule");
    time.setRequired(true);
    options.addOption(time);

    Option timeWindow = new Option((String) null, "timeWindow", true, "time window for IP capping rule");
    timeWindow.setRequired(true);
    options.addOption(timeWindow);

    Option updateTimeWindow = new Option((String) null, "updateTimeWindow", true, "update time window for IP capping rule");
    updateTimeWindow.setRequired(true);
    options.addOption(updateTimeWindow);

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

  /**
   * Filter rule entrance
   */
  @Override
  public void run() throws Exception {
    JavaRDD<IPCappingEvent> filteredRecords = filterEvents(this.table);
    writeFilteredEvents(filteredRecords);
  }

  /**
   * Filter IPCapping events. It reads all records from last time range. But only wirte records in last time window.
   * 1. Pick out click events.
   * 2. Group by records IP.
   * 3. Count each group to check if it is larger than threshold.
   * 4. Mark each record if it is passed.
   *    Failed Records: capping_failed_rule = IPCaIPCappingRule, capping_passed = false
   *    Passed Records: capping_failed_rule = None,              capping_passed = true
   *
   * @param table HBase table we read from
   * @return JavaRDD containing records in last time window.
   *
   */
  public JavaRDD<IPCappingEvent> filterEvents(String table) throws Exception {
    JavaPairRDD<String, IPCappingEvent> eventRDD = readEvents(table);
    JavaPairRDD<String, IPCappingEvent> clickRDD = eventRDD.filter(new FilterClick());
    JavaPairRDD<String, Iterable<IPCappingEvent>>  groupedClickRDD = clickRDD.groupByKey();
    JavaRDD<IPCappingEvent> resultRDD = groupedClickRDD.flatMap(new FilterOnIp());
    return resultRDD;
  }

  /**
   * Write records back to HBase
   * @param filteredRecords Last time window records which have already run the filter
   */
  public void writeFilteredEvents(JavaRDD<IPCappingEvent> filteredRecords) {
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = filteredRecords.mapToPair(new Event2HBasePutFunc());
    hbasePuts.foreachPartition(new HBasePutFunc());
  }

  /**
   * Read events by slices into JavaPairRDD with IP as Key, records with this IP as Value
   *
   * @param table HBase table to read from
   * @return JavaPariRDD containing the data in the scan range
   */
  public JavaPairRDD<String, IPCappingEvent> readEvents(final String table) {
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

          long timestampStart = time - timeWindow;
          long timestampStop = time;

          byte[] rowkeyStartBytes = generateIdentifier(timestampStart, slice.shortValue());
          byte[] rowkeyStopBytes = generateIdentifier(timestampStop, slice.shortValue());

          HBaseScanIterator hBaseScanIterator = new HBaseScanIterator(table, rowkeyStartBytes, rowkeyStopBytes);
          return hBaseScanIterator;
        }
      });

    JavaPairRDD<String, IPCappingEvent> pairRDD = javaRDD.mapToPair(new HBaseResult2EventFunc());
    return pairRDD;
  }

  /**
   * Read data from HBase and create JavaPairRDD with IP as key, Event as value.
   */
  public class HBaseResult2EventFunc implements PairFunction<Result, String, IPCappingEvent> {
    public Tuple2<String, IPCappingEvent> call(Result entry) throws Exception {
      IPCappingEvent ipCappingEvent = new IPCappingEvent();
      ipCappingEvent.setIdentifier(entry.getRow());
      ipCappingEvent.setChannelAction(Bytes.toString(entry.getValue(Bytes.toBytes("x"), Bytes.toBytes("channel_action"))));
      String ipAddress = Bytes.toString(entry.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_headers")));
      String[] ipStr = ipAddress.split("X-eBay-Client-IP:");
      if (ipStr.length > 1) {
        ipAddress = ipStr[1];
        ipAddress = ipAddress.split("\\|")[0].trim().replace(".", "");
      } else {
        ipAddress = "0";
      }
      return new Tuple2<String, IPCappingEvent>(ipAddress, ipCappingEvent);
    }
  }

  /**
   * IPCapping only cares click events
   */
  class FilterClick implements Function<Tuple2<String, IPCappingEvent>, Boolean> {
    @Override
    public Boolean call(Tuple2<String, IPCappingEvent> eventTuple) throws Exception {
      return eventTuple._2.getChannelAction().equals("CLICK");
    }
  }

  /**
   * Filter on IP count. We only return records whose timestamp are in the current time window range.
   */
  class FilterOnIp implements FlatMapFunction<Tuple2<String, Iterable<IPCappingEvent>>, IPCappingEvent> {
    @Override
    public Iterator<IPCappingEvent> call(Tuple2<String, Iterable<IPCappingEvent>> stringIterableTuple2) throws
    Exception {
      int count = 0;
      Iterator<IPCappingEvent> iterator1 = stringIterableTuple2._2.iterator();
      while(iterator1.hasNext()) {
        iterator1.next();
        count ++;
      }
      List<IPCappingEvent> result = new ArrayList<IPCappingEvent>();
      Iterator<IPCappingEvent> iterator2 = stringIterableTuple2._2.iterator();
      while(iterator2.hasNext()) {
        IPCappingEvent event = iterator2.next();
        if(count > threshold) {
          event.setCappingPassed(false);
          event.setCappingFailedRule(RULE_NAME);
        }
        else {
          event.setCappingPassed(true);
        }
        //Only write last 30 minutes data
        long eventTimestamp = getTimestampFromIdentifier(event.getIdentifier());
        if(eventTimestamp > updateWindowStartTime) {
          result.add(event);
        }
      }
      return result.iterator();
    }
  }

  /**
   * pairFunction used to map poj to HBase for writing
   */
  class Event2HBasePutFunc implements PairFunction<IPCappingEvent, ImmutableBytesWritable, Put> {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(IPCappingEvent event)
      throws Exception {
      Put put = new Put(event.getIdentifier());
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes(CAPPING_PASSED_COL),
        Bytes.toBytes(event.isCappingPassed()));
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes(CAPPING_FAILED_RULE_COL),
        Bytes.toBytes(event.getCappingFailedRule()));
      return new Tuple2<ImmutableBytesWritable, Put>(
        new ImmutableBytesWritable(), put);
    }
  }

  /**
   * Function used to write hbase
   */
  class HBasePutFunc implements VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>> {

    @Override
    public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> tupleIter) throws Exception {

      HTable transactionalTable = new HTable(TableName.valueOf(resultTable), HBaseConnection.getConnection());
      Tuple2<ImmutableBytesWritable, Put> tuple = null;
      while (tupleIter.hasNext()) {
        tuple = tupleIter.next();
        transactionalTable.put(tuple._2);
      }
      transactionalTable.close();
    }
  }

  /**
   * Generate 10 bytes row key
   *
   * @param timestamp timestamp
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

  /*
    Get timestamp from row key identifier
   */
  public long getTimestampFromIdentifier(byte[] identifier) {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.put(identifier);
    return buffer.getLong(2) >>> 24l | HIGH_24;
  }

  /**
   * Only for test
   */
  public static void setMod(short mod) {
    MOD = mod;
  }
}
