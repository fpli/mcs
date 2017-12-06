package com.ebay.traffic.chocolate.cappingrules.Rules;

import com.ebay.traffic.chocolate.cappingrules.AbstractCapper;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperEvent;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by yimeng on 11/12/17.
 */
public class TempSNIDCapper extends AbstractCapper {
  
  private SNIDCapper snidCapper;
  
  public TempSNIDCapper(String jobName, String mode, String originalTable, String resultTable, String startTime, String
      stopTime, String channelType) {
    super(jobName, mode, originalTable, resultTable, startTime, stopTime, channelType);
  }
  
  public TempSNIDCapper(String jobName, String mode, String originalTable, String resultTable, String startTime, String
      stopTime, String channelType, Integer updateTimeWindow) throws java.text.ParseException {
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
      formatter.printHelp("TempSNIDCappingRuleJob", options);
      System.exit(1);
      return;
    }
    
    TempSNIDCapper job = new TempSNIDCapper(cmd.getOptionValue("jobName"),
        cmd.getOptionValue("mode"), cmd.getOptionValue("originalTable"), cmd.getOptionValue("resultTable"), cmd
        .getOptionValue("startTime"), cmd.getOptionValue("endTime"),  cmd.getOptionValue("channelType"), Integer.valueOf(cmd
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
    
    snidCapper = new SNIDCapper(jobName(), mode(), originalTable, resultTable, startTime, stopTime, channelType);
    JavaPairRDD<Long, SNIDCapperEvent> filterResult = this.filterWithCapper(hbaseData);
    
    snidCapper.writeToHbase(filterResult, resultTable);
  }
  
  @Override
  public <T> T filterWithCapper(JavaRDD<Result> hbaseData) {
    JavaPairRDD<String, SNIDCapperEvent> snidCapperRDD = hbaseData.mapToPair(new ReadDataFromHase());
    
    JavaPairRDD<String, Iterable<SNIDCapperEvent>> groupbySnid = snidCapperRDD.groupByKey();
    
    JavaPairRDD<byte[], SNIDCapperEvent> resultRDD = groupbySnid.flatMapToPair(snidCapper.new FilterDataBySnid());
    return (T) resultRDD;
  }
  
  @Override
  public <T> void writeToHbase(T writeData, String table) {
  }
  
  public class ReadDataFromHase implements PairFunction<Result, String, SNIDCapperEvent> {
    public Tuple2<String, SNIDCapperEvent> call(Result entry) throws Exception {
      SNIDCapperEvent snidCapperEvent = new SNIDCapperEvent();
      snidCapperEvent.setRowIdentifier(entry.getRow());
      snidCapperEvent.setChannelAction(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY, Bytes.toBytes("channel_action"))));
      String ipAddress = Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY, Bytes.toBytes("request_headers")));
      String[] ipStr = ipAddress.split("X-eBay-Client-IP:");
      if (ipStr.length > 1) {
        ipAddress = ipStr[1];
        ipAddress = ipAddress.split("\\|")[0].trim().replace(".", "");
      } else {
        ipAddress = "0";
      }
      snidCapperEvent.setSnid(ipAddress);
      logger().info(" ----- ipAddress = " + ipAddress);
      return new Tuple2<String, SNIDCapperEvent>(ipAddress, snidCapperEvent);
    }
  }
}
