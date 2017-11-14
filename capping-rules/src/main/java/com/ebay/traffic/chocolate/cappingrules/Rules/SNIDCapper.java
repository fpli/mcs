package com.ebay.traffic.chocolate.cappingrules.Rules;

import com.ebay.traffic.chocolate.cappingrules.AbstractCapper;
import com.ebay.traffic.chocolate.cappingrules.constant.ChannelAction;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperIdentity;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperResult;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yimeng on 11/12/17.
 */
public class SNIDCapper extends AbstractCapper {
  static PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, SNIDCapperIdentity> readHBaseMapFunc = new
      PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, SNIDCapperIdentity>() {
        @Override
        public Tuple2<Long, SNIDCapperIdentity> call(
            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
          
          Result r = entry._2;
          long snid = Bytes.toLong(r.getValue(columnFamily, Bytes.toBytes("snid")));
          
          SNIDCapperIdentity snidIdentity = new SNIDCapperIdentity();
          snidIdentity.setSnapshotId(Bytes.toLong(r.getRow()));
          snidIdentity.setChannelAction(Bytes.toString(r.getValue(columnFamily, Bytes.toBytes("channel_action"))));
          snidIdentity.setSnid(snid);
          return new Tuple2<>(snid, snidIdentity);
        }
      };
  
  static PairFlatMapFunction<Tuple2<Long, Iterable<SNIDCapperIdentity>>, Long, SNIDCapperResult> filterGroupedRDD = new
      PairFlatMapFunction<Tuple2<Long, Iterable<SNIDCapperIdentity>>, Long, SNIDCapperResult>() {
        @Override
        public Iterator<Tuple2<Long, SNIDCapperResult>> call(Tuple2<Long, Iterable<SNIDCapperIdentity>> t)
            throws Exception {
          List<Tuple2<Long, SNIDCapperResult>> results = new ArrayList<Tuple2<Long, SNIDCapperResult>>();
          Iterator<SNIDCapperIdentity> ite1 = t._2.iterator();
          long impSnapshotId = 0;
          long impTime = Long.MAX_VALUE;
          SNIDCapperIdentity impEvent = null;
          while (ite1.hasNext()) {
            impEvent = ite1.next();
            if (ChannelAction.IMPRESSION.name().equalsIgnoreCase(impEvent.getChannelAction())) {
              impSnapshotId = impEvent.getSnapshotId();
              impTime = getTimeMillis(impSnapshotId);
              break;
            }
          }
          
          Iterator<SNIDCapperIdentity> ite2 = t._2.iterator();
          SNIDCapperIdentity clickEvent = null;
          SNIDCapperResult resultEvent = null;
          while (ite2.hasNext()) {
            clickEvent = ite2.next();
            if (ChannelAction.CLICK.name().equalsIgnoreCase(clickEvent.getChannelAction())) {
              if (getTimeMillis(clickEvent.getSnapshotId()) <= impTime) {
                resultEvent.setSnapshotId(clickEvent.getSnapshotId());
                resultEvent.setImpressed(false);
                resultEvent.setImpSnapshotId(impSnapshotId);
                results.add(new Tuple2<>(resultEvent.getSnapshotId(), resultEvent));
              }
            }
          }
          return results.iterator();
        }
      };
  static PairFunction<SNIDCapperResult, ImmutableBytesWritable, Put> writeHBaseMapFunc = new PairFunction<SNIDCapperResult,
      ImmutableBytesWritable, Put>() {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(SNIDCapperResult snidResult)
        throws Exception {
      Put put = new Put(Bytes.toBytes(snidResult.getSnapshotId()));
      put.add(columnFamily, Bytes.toBytes("is_impressed"), Bytes.toBytes(snidResult.getImpressed()));
      put.add(columnFamily, Bytes.toBytes("imp_snapshot_id"), Bytes.toBytes(snidResult.getImpSnapshotId()));
      
      return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }
  };
  
  public SNIDCapper(String jobName, String mode, String originalTable, String resultTable, String startTime, String
      endTime) throws java.text.ParseException {
    super(jobName, mode, originalTable, resultTable, startTime, endTime);
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
        .getOptionValue("startTime"), cmd.getOptionValue("endTime"));
    try {
      job.run();
    } finally {
      job.stop();
    }
  }
  
  @Override
  public <T> void writeToHbase(T writeData, String table) {
    
    Job newAPIJobConfiguration = null;
    try {
      newAPIJobConfiguration = Job.getInstance(hbaseConf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
    newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
    
    JavaPairRDD<Long, SNIDCapperResult> filterResult = (JavaPairRDD<Long, SNIDCapperResult>) writeData;
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = filterResult.values().mapToPair(writeHBaseMapFunc);
    hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
  }
  
  @Override
  public <T> T filterWithCapper(JavaPairRDD<ImmutableBytesWritable, Result> hbaseData) {
    JavaPairRDD<Long, SNIDCapperIdentity> snidCapperRDD = hbaseData.mapToPair(readHBaseMapFunc);
    
    //Group by session id
    JavaPairRDD<Long, Iterable<SNIDCapperIdentity>> groupbySnid = snidCapperRDD.groupByKey();
    
    JavaPairRDD<Long, SNIDCapperResult> resultRDD = groupbySnid.flatMapToPair(filterGroupedRDD);
    return (T) resultRDD;
  }
}
