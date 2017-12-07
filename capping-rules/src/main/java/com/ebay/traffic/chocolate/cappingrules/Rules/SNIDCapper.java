package com.ebay.traffic.chocolate.cappingrules.Rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.traffic.chocolate.cappingrules.AbstractCapper;
import com.ebay.traffic.chocolate.cappingrules.IdentifierUtil;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperEvent;
import jodd.util.StringUtil;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SNID Capping Rule is to identify if the click could uniquely tie back to its originating impression.
 * SNID = SessionId which is the unique id to tie impress and click
 * <p>
 * <p>
 * Created by yimeng on 11/12/17.
 */
public class SNIDCapper extends AbstractCapper {
  
  /**
   * Default Constructor without updateTimeWindow which will update all scanned data
   *
   * @param jobName       spark job name
   * @param mode          spark submit mode
   * @param originalTable HBase table which data queried from
   * @param resultTable   HBase table which data stored in
   * @param startTime     scan start time
   * @param stopTime      scan stop time
   * @param channelType   marketing channel like EPN, DAP, SEARCH
   */
  public SNIDCapper(String jobName, String mode, String originalTable, String resultTable, String startTime, String
      stopTime, String channelType) {
    super(jobName, mode, originalTable, resultTable, startTime, stopTime, channelType);
  }
  
  /**
   * Constructor for SNID Capping Rule with updateTimeWindow
   *
   * @param jobName          spark job name
   * @param mode             spark submit mode
   * @param originalTable    HBase table which data queried from
   * @param resultTable      HBase table which data stored in
   * @param startTime        scan start time
   * @param stopTime         scan stop time
   * @param channelType      marketing channel like EPN, DAP, SEARCH
   * @param updateTimeWindow HBase data update time window
   */
  public SNIDCapper(String jobName, String mode, String originalTable, String resultTable, String startTime, String
      stopTime, String channelType, int updateTimeWindow) throws java.text.ParseException {
    super(jobName, mode, originalTable, resultTable, startTime, stopTime, channelType, updateTimeWindow);
  }
  
  /**
   * Main function. Get parameters and then run the job.
   *
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Options options = getJobOptions("ClickImp Mapping Rule");
    
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
        cmd.getOptionValue("mode"), cmd.getOptionValue("originalTable"), cmd.getOptionValue("resultTable"), cmd
        .getOptionValue("startTime"), cmd.getOptionValue("endTime"), cmd.getOptionValue("channelType"), Integer.valueOf(cmd
        .getOptionValue("updateTimeWindow")));
    try {
      job.run();
    } finally {
      job.stop();
    }
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
    // scan data from HBase
    JavaRDD<Result> hbaseData = readFromHabse();
    
    // filter data by SNID Capping Rule
    JavaPairRDD<Long, SNIDCapperEvent> filterResult = filterWithCapper(hbaseData);
    
    // write data back to HBase
    writeToHbase(filterResult);
  }
  
  /**
   * Filter Data by SNID Capping rules
   * Step1: transform sessionId as the row key instead of default row identifier
   * Step2: group by session id
   * Step3: transform filtered data to HBase raw data
   *
   * @param hbaseData scanned HBase data
   * @return filter result data
   */
  @Override
  public <T> T filterWithCapper(JavaRDD<Result> hbaseData) {
    JavaPairRDD<String, SNIDCapperEvent> snidCapperRDD = hbaseData.mapToPair(new ReadDataFromHase());
    
    JavaPairRDD<String, Iterable<SNIDCapperEvent>> groupbySnid = snidCapperRDD.groupByKey();
    
    JavaPairRDD<byte[], SNIDCapperEvent> resultRDD = groupbySnid.flatMapToPair(new FilterDataBySnid());
    return (T) resultRDD;
  }
  
  /**
   * Write filtered data back to HBase
   *
   * @param writeData filtered data
   */
  @Override
  public <T> void writeToHbase(T writeData) {
    
    JavaPairRDD<byte[], SNIDCapperEvent> filterResult = (JavaPairRDD<byte[], SNIDCapperEvent>) writeData;
    
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = filterResult.values().mapToPair(new WriteHBaseMap());
    
    hbasePuts.foreachPartition(new PutDataToHase());
  }
  
  /**
   * transform sessionId as the row key instead of default row identifier
   */
  public class ReadDataFromHase implements PairFunction<Result, String, SNIDCapperEvent> {
    public Tuple2<String, SNIDCapperEvent> call(Result entry) throws Exception {
      Result r = entry;
      String snid = Bytes.toString(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("snid")));
      
      SNIDCapperEvent snidCapperEvent = new SNIDCapperEvent();
      snidCapperEvent.setRowIdentifier(r.getRow());
      snidCapperEvent.setChannelAction(Bytes.toString(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("channel_action"))));
      snidCapperEvent.setSnid(snid);
      return new Tuple2<String, SNIDCapperEvent>(snid, snidCapperEvent);
    }
  }
  
  /**
   * Filter data by session Id
   * step1: tie click to impression
   * step2: identify if the click happened after impression
   * step3: only store data in update time window
   */
  public class FilterDataBySnid implements PairFlatMapFunction<Tuple2<String, Iterable<SNIDCapperEvent>>, byte[],
      SNIDCapperEvent> {
    public Iterator<Tuple2<byte[], SNIDCapperEvent>> call(Tuple2<String, Iterable<SNIDCapperEvent>> t)
        throws Exception {
      List<Tuple2<byte[], SNIDCapperEvent>> results = new ArrayList<Tuple2<byte[], SNIDCapperEvent>>();
      Iterator<SNIDCapperEvent> snidEventIte1 = t._2.iterator();
      byte[] impRowIdentifier = null;
      long impTime = Long.MAX_VALUE;
      ;
      SNIDCapperEvent impEvent = null;
      while (snidEventIte1.hasNext()) {
        impEvent = snidEventIte1.next();
        if (StringUtil.isEmpty(impEvent.getSnid())) {
          continue;
        }
        if (ChannelAction.IMPRESSION.name().equalsIgnoreCase(impEvent.getChannelAction())) {
          impRowIdentifier = impEvent.getRowIdentifier();
          impTime = IdentifierUtil.getTimeMillisFromRowkey(impRowIdentifier);
          break;
        }
      }
      
      Iterator<SNIDCapperEvent> snidEventIte2 = t._2.iterator();
      SNIDCapperEvent clickEvent = null;
//      long stopTimestampWindow = new SimpleDateFormat(INPUT_DATE_FORMAT).parse(stopTime).getTime();
//      stopTimestampWindow = stopTimestampWindow - updateTimeWindow * 60 * 1000;
      long clickTimestamp = 0;
      byte[] clickRowIdentifier = null;
      while (snidEventIte2.hasNext()) {
        clickEvent = snidEventIte2.next();
        clickRowIdentifier = clickEvent.getRowIdentifier();
        if (StringUtil.isEmpty(impEvent.getSnid())) {
          continue;
        }
        if (ChannelAction.CLICK.name().equalsIgnoreCase(clickEvent.getChannelAction())) {
          if (clickEvent.isImpressed()) {
            continue;
          }
          clickTimestamp = IdentifierUtil.getTimeMillisFromRowkey(clickRowIdentifier);
          //only write latest clicks by time window
          if (updateWindowStartTime > 0 && clickTimestamp < updateWindowStartTime) {
            continue;
          }
          if (clickTimestamp > impTime) {
            clickEvent.setImpressed(true);
            clickEvent.setImpRowIdentifier(impRowIdentifier);
          } else {
            clickEvent.setImpressed(false);
          }
          results.add(new Tuple2<byte[], SNIDCapperEvent>(clickRowIdentifier, clickEvent));
        }
      }
      return results.iterator();
    }
  }
  
  /**
   * Write data with capping flag back to HBase
   */
  public class WriteHBaseMap implements PairFunction<SNIDCapperEvent, ImmutableBytesWritable, Put> {
    public Tuple2<ImmutableBytesWritable, Put> call(SNIDCapperEvent snidCapperEvent)
        throws Exception {
      Put put = new Put(snidCapperEvent.getRowIdentifier());
      put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("is_impressed"), Bytes.toBytes(snidCapperEvent.isImpressed()));
      put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("imp_row_key"), snidCapperEvent.getImpRowIdentifier());
      return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }
  }
}
