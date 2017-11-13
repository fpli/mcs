package com.ebay.traffic.chocolate.cappingrules;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import java.io.IOException;

/**
 * Created by yimeng on 11/13/17.
 */
public interface Capper {
  
  JavaPairRDD<ImmutableBytesWritable, Result> readFromHabse(String table, long startTimestamp, long
      stopTimestamp) throws IOException,
      ServiceException;
  
  void writeToHbase(JavaPairRDD<Long, Event> result, String table, PairFunction<Event, ImmutableBytesWritable,
      Put> writeHBaseMapFunc);
  
  JavaPairRDD<Long, Event> filterWithCapper(JavaPairRDD<ImmutableBytesWritable, Result> hbaseData);
  
  //void run() throws Exception;
}
