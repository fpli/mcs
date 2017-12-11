package com.ebay.traffic.chocolate.cappingrules.Rules;

import com.ebay.traffic.chocolate.cappingrules.AbstractCapper;
import com.ebay.traffic.chocolate.cappingrules.HBaseConnection;
import com.ebay.traffic.chocolate.cappingrules.IdentifierUtil;
import com.ebay.traffic.chocolate.cappingrules.dto.IPCapperEvent;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The IPCappingRule is to check whether there are too many clicks
 * from specific IP address during a certain period.
 *
 * @author xiangli4
 */
public class IPCapper extends AbstractCapper {
  // Rule name. It will be written to HBase if one record does not pass.
  private final String RULE_NAME = "IPCappingRule";
  // Capping passed column name.
  private final String CAPPING_PASSED_COL = "capping_passed";
  // Capping failed rule column name.
  private final String CAPPING_FAILED_RULE_COL = "capping_failed_rule";
  // IP count threshold. Used to judge is an IP is invalid
  private final long threshold;
  
  /**
   * Constructor for IP Capping Rule
   *
   * @param jobName          spark job name
   * @param mode             spark submit mode
   * @param originalTable    HBase table which data queried from
   * @param resultTable      HBase table which data stored in
   * @param channelType      marketing channel like EPN, DAP, SEARCH
   * @param scanStopTime     scan stop time
   * @param scanTimeWindow   scan time window (minutes)
   * @param updateTimeWindow HBase data update time window (minutes)
   * @param threshold        IP Capping rule threshold
   */
  public IPCapper(String jobName, String mode, String originalTable, String resultTable, String channelType, String
      scanStopTime, int scanTimeWindow, int updateTimeWindow, long threshold) throws java.text.ParseException {
    super(jobName, mode, originalTable, resultTable, channelType, scanStopTime, scanTimeWindow, updateTimeWindow);
    this.threshold = threshold;
  }
  
  /**
   * Main function. Get parameters and then run the job.
   *
   * @param args
   */
  public static void main(String[] args) throws java.text.ParseException {
    CommandLine cmd = parseOptions(args);
  
    IPCapper job = new IPCapper(cmd.getOptionValue("jobName"),
        cmd.getOptionValue("mode"), cmd.getOptionValue("originalTable"), cmd.getOptionValue("resultTable"), cmd
        .getOptionValue("channelType"), cmd.getOptionValue("scanStopTime"), Integer.valueOf(cmd.getOptionValue
        ("scanTimeWindow")), Integer.valueOf(cmd.getOptionValue("updateTimeWindow")), Long.valueOf(cmd.getOptionValue("threshold")));
    
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
   *
   * @param args arguments
   * @return CommandLine used to get every argument
   */
  public static CommandLine parseOptions(String[] args) {
    Options options = getJobOptions("IP Capping Rule");
    
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
   * Run SNID Capping Rule
   * step1 : scan data from HBase
   * step2: filter data by SNID Capping Rule
   * step3: write data back to HBase
   *
   * @throws Exception job runtime exception
   */
  @Override
  public void run() throws Exception {
    JavaRDD<Result> hbaseData = readFromHabse();
    
    JavaRDD<IPCapperEvent> filterResult = filterWithCapper(hbaseData);
    
    writeToHbase(filterResult);
  }
  
  /**
   * Filter IPCapping events. It reads all records from last time range. But only wirte records in last time window.
   * 1. Pick out click events.
   * 2. Group by records IP.
   * 3. Count each group to check if it is larger than threshold.
   * 4. Mark each record if it is passed.
   * Failed Records: capping_failed_rule = IPCaIPCappingRule, capping_passed = false
   * Passed Records: capping_failed_rule = None,              capping_passed = true
   *
   * @param hbaseData the data read from HBase
   * @param <T>       JavaRDD containing records in last time window.
   * @return
   */
  @Override
  protected <T> T filterWithCapper(JavaRDD<Result> hbaseData) {
    JavaPairRDD<String, IPCapperEvent> eventRDD = hbaseData.mapToPair(new HBaseResult2EventFunc());
    JavaPairRDD<String, IPCapperEvent> clickRDD = eventRDD.filter(new FilterClick());
    JavaPairRDD<String, Iterable<IPCapperEvent>> groupedClickRDD = clickRDD.groupByKey();
    JavaRDD<IPCapperEvent> resultRDD = groupedClickRDD.flatMap(new FilterOnIp());
    return (T) resultRDD;
  }
  
  /**
   * Write records back to HBase
   *
   * @param writeData the filtered records which need to be wrote back to HBase
   */
  @Override
  public <T> void writeToHbase(T writeData) {
    JavaRDD<IPCapperEvent> filterResult = (JavaRDD<IPCapperEvent>) writeData;
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = filterResult.mapToPair(new Event2HBasePutFunc());
    hbasePuts.foreachPartition(new HBasePutFunc());
  }
  
  /**
   * Read data from HBase and create JavaPairRDD with IP as key, Event as value.
   */
  public class HBaseResult2EventFunc implements PairFunction<Result, String, IPCapperEvent> {
    public Tuple2<String, IPCapperEvent> call(Result entry) throws Exception {
      IPCapperEvent ipCapperEvent = new IPCapperEvent();
      ipCapperEvent.setRowIdentifier(entry.getRow());
      ipCapperEvent.setChannelAction(Bytes.toString(entry.getValue(Bytes.toBytes("x"), Bytes.toBytes("channel_action"))));
      String ipAddress = Bytes.toString(entry.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_headers")));
      String[] ipStr = ipAddress.split("X-eBay-Client-IP:");
      if (ipStr.length > 1) {
        ipAddress = ipStr[1];
        ipAddress = ipAddress.split("\\|")[0].trim().replace(".", "");
      } else {
        ipAddress = "0";
      }
      return new Tuple2<String, IPCapperEvent>(ipAddress, ipCapperEvent);
    }
  }
  
  /**
   * IPCapping only cares click events
   */
  class FilterClick implements Function<Tuple2<String, IPCapperEvent>, Boolean> {
    @Override
    public Boolean call(Tuple2<String, IPCapperEvent> eventTuple) throws Exception {
      return eventTuple._2.getChannelAction().equals("CLICK");
    }
  }
  
  /**
   * Filter on IP count. We only return records whose timestamp are in the current time window range.
   */
  class FilterOnIp implements FlatMapFunction<Tuple2<String, Iterable<IPCapperEvent>>, IPCapperEvent> {
    @Override
    public Iterator<IPCapperEvent> call(Tuple2<String, Iterable<IPCapperEvent>> stringIterableTuple2) throws
        Exception {
      int count = 0;
      Iterator<IPCapperEvent> iterator1 = stringIterableTuple2._2.iterator();
      while (iterator1.hasNext()) {
        iterator1.next();
        count++;
      }
      List<IPCapperEvent> result = new ArrayList<IPCapperEvent>();
      Iterator<IPCapperEvent> iterator2 = stringIterableTuple2._2.iterator();
      while (iterator2.hasNext()) {
        IPCapperEvent event = iterator2.next();
        if (count > threshold) {
          event.setCappingPassed(false);
          event.setCappingFailedRule(RULE_NAME);
        } else {
          event.setCappingPassed(true);
        }
        //Only write last 30 minutes data
        long eventTimestamp = IdentifierUtil.getTimeMillisFromRowkey(event.getRowIdentifier());
        if (eventTimestamp > updateWindowStartTime) {
          result.add(event);
        }
      }
      return result.iterator();
    }
  }
  
  /**
   * pairFunction used to map poj to HBase for writing
   */
  class Event2HBasePutFunc implements PairFunction<IPCapperEvent, ImmutableBytesWritable, Put> {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(IPCapperEvent event)
        throws Exception {
      Put put = new Put(event.getRowIdentifier());
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
}
