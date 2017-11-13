package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.BaseSparkJob;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yimeng on 11/12/17.
 */
public class SNIDCapper extends BaseSparkJob {
  // The high 26f bits of system time - which means our snapshots are valid from 24 Sep 2015
  protected static final long HIGH_24 = 0x15000000000l;
  private final static byte[] columnX = Bytes.toBytes("x");
  
  static PairFunction<Event, ImmutableBytesWritable, Put> writeHBaseMapFunc = new PairFunction<Event,
      ImmutableBytesWritable, Put>() {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Event event)
        throws Exception {
      Put put = new Put(Bytes.toBytes(event.getSnapshotId()));
      put.add(Bytes.toBytes("x"), Bytes.toBytes("is_impressed"), Bytes.toBytes(event.getImpressed()));
      put.add(Bytes.toBytes("x"), Bytes.toBytes("imp_snapshot_id"), Bytes.toBytes(event.getImpSnapshotId()));
      
      return new Tuple2<ImmutableBytesWritable, Put>(
          new ImmutableBytesWritable(), put);
    }
  };
  static PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, Event> readHBaseMapFunc = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, Event>() {
    @Override
    public Tuple2<Long, Event> call(
        Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
      
      Result r = entry._2;
      long snid = Bytes.toLong(r.getValue(columnX, Bytes.toBytes("snid")));
      
      Event event = new Event();
      event.setSnapshotId(Bytes.toLong(r.getRow()));
      event.setChannelAction(Bytes.toString(r.getValue(columnX, Bytes.toBytes("channel_action"))));
      event.setSnid(snid);
      return new Tuple2<>(snid, event);
    }
  };
  
  //Filter by snid capper rule
  static PairFlatMapFunction<Tuple2<Long, Iterable<Event>>, Long, Event> mapGroupedRDD = new
      PairFlatMapFunction<Tuple2<Long, Iterable<Event>>, Long, Event>() {
        @Override
        public Iterator<Tuple2<Long, Event>> call(Tuple2<Long, Iterable<Event>> t)
            throws Exception {
          List<Tuple2<Long, Event>> results = new ArrayList<Tuple2<Long, Event>>();
          Iterator<Event> ite1 = t._2.iterator();
          long impSnapshotId = 0;
          //boolean isImpressed = false;
          long impTime = Long.MAX_VALUE;
          Event impEvent = null;
          while (ite1.hasNext()) {
            impEvent = ite1.next();
            if ("IMPRESSION".equalsIgnoreCase(impEvent.getChannelAction())) {
              //isImpressed = true;
              impSnapshotId = impEvent.getSnapshotId();
              impTime = getTimeMillis(impSnapshotId);
              break;
            }
          }
          
          Iterator<Event> ite2 = t._2.iterator();
          Event clickEvent = null;
          while (ite2.hasNext()) {
            clickEvent = ite2.next();
            if ("CLICK".equalsIgnoreCase(clickEvent.getChannelAction())) {
              if (getTimeMillis(clickEvent.getSnapshotId()) <= impTime) {
                clickEvent.setImpSnapshotId(impSnapshotId);
                clickEvent.setImpressed(false);
                results.add(new Tuple2<>(clickEvent.getSnapshotId(), clickEvent));
              }
            }
          }
          return results.iterator();
        }
      };
  private final Logger logger = LoggerFactory.getLogger(SNIDCapper.class);
  // the conditions to select data from hbase
  private final String table;
  private final long time;
  private final long timeRange;
  // environment context
  protected Configuration hbaseConf;
  /**
   * Only for test
   */
  private long numberOfRow;
  private JavaPairRDD<Long, Event> filteredResult;
  
  
  public SNIDCapper(String jobName, String mode, String table, long time, long timeRange) {
    super(jobName, mode, false);
    this.table = table;
    this.time = time;
    this.timeRange = timeRange;
  }
  
  public static void main(String[] args) throws Exception {
    Options options = GenericCappingJob.getJobOptions("ClickImp Mapping Rule");
    
    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("SNIDCappingRuleJob", options);
      
      System.exit(1);
      return;
    }
    
    SNIDCapper job = new SNIDCapper(cmd.getOptionValue("jobName"),
        cmd.getOptionValue("mode"), cmd.getOptionValue("table"), Long.parseLong(cmd.getOptionValue("time")),
        Long.parseLong(cmd.getOptionValue("timeRange")));
    try {
      job.run();
    } finally {
      job.stop();
    }
  }
  
  private static long getTimeMillis(long snapshotId) {
    return snapshotId >>> 24l | HIGH_24;
  }
  
  /**
   * run capping job
   */
  @Override
  public void run() throws Exception {
    GenericCappingJob genericCappingJob = new GenericCappingJob(hbaseConf, jsc());
    long startTimestamp = time - timeRange;
    JavaPairRDD<ImmutableBytesWritable, Result> hbaseData = genericCappingJob.readFromHabse(table,
        startTimestamp, time);
    
    JavaPairRDD<Long, Event> filterResult = filterWithCapper(hbaseData);
    //TODO write to testing table
    genericCappingJob.writeToHbase(filterResult, "snid_capping_result", writeHBaseMapFunc);
  }
  
  /**
   * Filter ipcapping events
   *
   * @param hbaseData selected data from hbase
   * @return invalids events which don't pass the capping rule
   */
  
  public JavaPairRDD<Long, Event> filterWithCapper(JavaPairRDD<ImmutableBytesWritable, Result> hbaseData) {
    JavaPairRDD<Long, Event> snidCapperRDD = hbaseData.mapToPair(readHBaseMapFunc);
    
    //Group by session id
    JavaPairRDD<Long, Iterable<Event>> groupbySnid = snidCapperRDD.groupByKey();
    
    JavaPairRDD<Long, Event> resultRDD = groupbySnid.flatMapToPair(mapGroupedRDD);
    return resultRDD;
  }
  
  /**
   * Only for test
   */
  public void setHBaseConf(Configuration hbaseConf) {
    this.hbaseConf = hbaseConf;
  }
}
