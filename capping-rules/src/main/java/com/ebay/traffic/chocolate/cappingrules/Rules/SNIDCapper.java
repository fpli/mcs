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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yimeng on 11/12/17.
 */
public class SNIDCapper extends AbstractCapper {
  
  public SNIDCapper(String jobName, String mode, String originalTable, String resultTable, String startTime, String
      stopTime, String channelType) {
    super(jobName, mode, originalTable, resultTable, startTime, stopTime, channelType);
  }
  
  public SNIDCapper(String jobName, String mode, String originalTable, String resultTable, String startTime, String
      stopTime, String channelType, int updateTimeWindow) throws java.text.ParseException {
    super(jobName, mode, originalTable, resultTable, startTime, stopTime, channelType, updateTimeWindow);
  }
  
  
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
  
  @Override
  public void run() throws Exception {
    
    JavaRDD<Result> hbaseData = readFromHabse();
    
    JavaPairRDD<Long, SNIDCapperEvent> filterResult = filterWithCapper(hbaseData);
    
    writeToHbase(filterResult, resultTable);
  }
  
  @Override
  public <T> T filterWithCapper(JavaRDD<Result> hbaseData) {
    JavaPairRDD<String, SNIDCapperEvent> snidCapperRDD = hbaseData.mapToPair(new ReadDataFromHase());
    
    JavaPairRDD<String, Iterable<SNIDCapperEvent>> groupbySnid = snidCapperRDD.groupByKey();
    
    JavaPairRDD<byte[], SNIDCapperEvent> resultRDD = groupbySnid.flatMapToPair(new FilterDataBySnid());
    return (T) resultRDD;
  }
  
  @Override
  public <T> void writeToHbase(T writeData, String table) {
    
    JavaPairRDD<byte[], SNIDCapperEvent> filterResult = (JavaPairRDD<byte[], SNIDCapperEvent>) writeData;
    
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = filterResult.values().mapToPair(new WriteHBaseMap());
    
    hbasePuts.foreachPartition(new PutDataToHase());
  }
  
  public class ReadDataFromHase implements PairFunction<Result, String, SNIDCapperEvent> {
    public Tuple2<String, SNIDCapperEvent> call(Result entry) throws Exception {
      Result r = entry;
      String snid = Bytes.toString(r.getValue(HBaseConstant.COLUMN_FAMILY, Bytes.toBytes("snid")));
      
      SNIDCapperEvent snidCapperEvent = new SNIDCapperEvent();
      snidCapperEvent.setRowIdentifier(r.getRow());
      snidCapperEvent.setChannelAction(Bytes.toString(r.getValue(HBaseConstant.COLUMN_FAMILY, Bytes.toBytes("channel_action"))));
      snidCapperEvent.setSnid(snid);
      return new Tuple2<String, SNIDCapperEvent>(snid, snidCapperEvent);
    }
  }
  
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
      long stopTimestampWindow = new SimpleDateFormat(INPUT_DATE_FORMAT).parse(stopTime).getTime();
      stopTimestampWindow = stopTimestampWindow - updateTimeWindow * 60 * 1000;
      long clickTimestamp = 0;
      while (snidEventIte2.hasNext()) {
        clickEvent = snidEventIte2.next();
        if (StringUtil.isEmpty(impEvent.getSnid())) {
          continue;
        }
        if (ChannelAction.CLICK.name().equalsIgnoreCase(clickEvent.getChannelAction())) {
          if (clickEvent.isImpressed()) {
            continue;
          }
          clickTimestamp = IdentifierUtil.getTimeMillisFromRowkey(clickEvent.getRowIdentifier());
          //only write latest clicks by time window
          if(updateTimeWindow > 0 && clickTimestamp < stopTimestampWindow){
            continue;
          }
          if (clickTimestamp > impTime) {
            clickEvent.setImpressed(true);
            clickEvent.setImpRowIdentifier(impRowIdentifier);
          } else {
            clickEvent.setImpressed(false);
          }
          results.add(new Tuple2<byte[], SNIDCapperEvent>(clickEvent.getRowIdentifier(), clickEvent));
        }
      }
      return results.iterator();
    }
  }
  
  public class WriteHBaseMap implements PairFunction<SNIDCapperEvent, ImmutableBytesWritable, Put> {
    public Tuple2<ImmutableBytesWritable, Put> call(SNIDCapperEvent snidCapperEvent)
        throws Exception {
      Put put = new Put(snidCapperEvent.getRowIdentifier());
      put.add(HBaseConstant.COLUMN_FAMILY, Bytes.toBytes("is_impressed"), Bytes.toBytes(snidCapperEvent.isImpressed()));
      put.add(HBaseConstant.COLUMN_FAMILY, Bytes.toBytes("imp_row_key"), snidCapperEvent.getImpRowIdentifier());
      return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }
  }
}
