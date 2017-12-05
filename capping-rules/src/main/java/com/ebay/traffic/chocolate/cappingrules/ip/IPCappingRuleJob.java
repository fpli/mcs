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
  private final Logger logger;
  private final String table;
  private final String resultTable;
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
   * @param resultTable result table
   * @param time      scan stop time
   * @param timeRange time range from start time to end time
   * @param threshold ip occurance threshold
   */
  public IPCappingRuleJob(String jobName, String mode, String table, String resultTable, long time, long timeRange, long threshold) {
    super(jobName, mode, false);
    this.table = table;
    this.resultTable = resultTable;
    this.time = time;
    this.timeRange = timeRange;
    this.threshold = threshold;
    this.logger = logger();
  }

  /**
   * Main function
   * @param args
   */
  public static void main(String[] args) {
    CommandLine cmd = parseOptions(args);
    IPCappingRuleJob job = new IPCappingRuleJob(cmd.getOptionValue("jobName"),
      cmd.getOptionValue("mode"), cmd.getOptionValue("table"), cmd.getOptionValue("resultTable"), Long.parseLong(cmd.getOptionValue("time")),
      Long.parseLong(cmd.getOptionValue("timeRange")), Long.parseLong(cmd.getOptionValue("threshold")));
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

  /**
   * Filter rule entrance
   */
  @Override
  public void run() throws Exception {
    JavaRDD<IPCappingEvent> filteredRecords = filterEvents(this.table);
    writeFilteredEvents(filteredRecords);
  }

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
   * Filter ipcapping events
   *
   * @param table hbase table
   * @return invalids events which don't pass the capping rule
   */
  public JavaRDD<IPCappingEvent> filterEvents(String table) throws Exception {
    JavaPairRDD<String, IPCappingEvent> pairRDD = readEvents(table);
    JavaPairRDD<String, IPCappingEvent> clickRDD = pairRDD.filter(new FilterClick());
    JavaPairRDD<String, Iterable<IPCappingEvent>>  groupByIpRDD = clickRDD.groupByKey();
    JavaRDD<IPCappingEvent> resultRDD = groupByIpRDD.flatMap(new FilterOnIp());
    return resultRDD;
  }

  /**
   * Write records back to hbase
   * @param filteredRecords
   */
  public void writeFilteredEvents(JavaRDD<IPCappingEvent> filteredRecords) {
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = filteredRecords.mapToPair(new WriteHBaseMapFunc());
    hbasePuts.foreachPartition(new HBasePutFunc());
  }

  /**
   * Read events into dataframe
   *
   * @param table hbase table
   * @return dataframe containing the data in the scan range
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

          long timestampStart = time - timeRange;
          long timestampStop = time;

          byte[] rowkeyStartBytes = generateIdentifier(timestampStart, slice.shortValue());
          byte[] rowkeyStopBytes = generateIdentifier(timestampStop, slice.shortValue());

          HBaseScanIterator hBaseScanIterator = new HBaseScanIterator(table, rowkeyStartBytes, rowkeyStopBytes);
          return hBaseScanIterator;
        }
      });

    JavaPairRDD<String, IPCappingEvent> pairRDD = javaRDD.mapToPair(new ReadDataFromHase());
    return pairRDD;
  }

  /**
   * Read data from hbase and create JavaPairRDD with IP as key, Event as value
   */
  public class ReadDataFromHase implements PairFunction<Result, String, IPCappingEvent> {
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
   * Only need Click Events
   */
  class FilterClick implements Function<Tuple2<String, IPCappingEvent>, Boolean> {

    @Override
    public Boolean call(Tuple2<String, IPCappingEvent> eventTuple) throws Exception {
      return eventTuple._2.getChannelAction().equals("CLICK");
    }
  }

  /**
   * Filter on IP count
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
          event.setCappingFailedRule("IPCappingRule");
        }
        else {
          event.setCappingPassed(true);
        }
        result.add(event);
      }
      return result.iterator();
    }
  }

  /**
   * pairFunction used to map poj to hbase for writing
   */
  class WriteHBaseMapFunc implements PairFunction<IPCappingEvent, ImmutableBytesWritable, Put> {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(IPCappingEvent event)
      throws Exception {
      Put put = new Put(event.getIdentifier());
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes("capping_passed"),
        Bytes.toBytes(event.isCappingPassed()));
      put.add(Bytes.toBytes("x"),
        Bytes.toBytes("capping_failed_rule"),
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
   * Only for test
   */
  public static void setMod(short mod) {
    MOD = mod;
  }
}
