package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.HBaseConnection;
import com.ebay.traffic.chocolate.cappingrules.cassandra.ApplicationOptions;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import com.ebay.traffic.chocolate.report.cassandra.RawReportRecord;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HBaseStorage implements IStorage<JavaRDD<List<RawReportRecord>>> {
  private static final Logger logger = LoggerFactory.getLogger(ApplicationOptions.class);
  private static String resultTable;
  private static HBaseStorage hBaseStorage;
  
  private HBaseStorage(){}
  
  public static HBaseStorage getInstance(String _resultTable){
    if(hBaseStorage == null){
      hBaseStorage = new HBaseStorage();
    }
    resultTable = _resultTable;
    return hBaseStorage;
  }
  
  @Override
  public void writeToStorage(JavaRDD<List<RawReportRecord>> reportRecords) {
    JavaRDD<List<RawReportRecord>> resultRDD = (JavaRDD<List<RawReportRecord>>) reportRecords;
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = resultRDD.flatMapToPair(new WriteHBaseMap());
    hbasePuts.foreachPartition(new PutDataToHase());
  }
  
  
  /**
   * write report data to HBase result table when storage type is HBASE
   */
  public class WriteHBaseMap implements PairFlatMapFunction<List<RawReportRecord>, ImmutableBytesWritable, Put> {
    public Iterator<Tuple2<ImmutableBytesWritable, Put>> call(List<RawReportRecord> reportRecordList)
        throws Exception {
      
      List<Tuple2<ImmutableBytesWritable, Put>> recordList = new ArrayList<Tuple2<ImmutableBytesWritable, Put>>();
      for (RawReportRecord reportRecord : reportRecordList) {
        Put put = new Put(Bytes.toBytes(reportRecord.getId()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("month"), Bytes.toBytes(reportRecord.getMonth()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("day"), Bytes.toBytes(reportRecord.getDay()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("timestamp"), Bytes.toBytes(reportRecord.getTimestamp()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("snapshot_id"), Bytes.toBytes(reportRecord.getSnapshotId()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("gross_clicks"), Bytes.toBytes(reportRecord.getGrossClicks()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("clicks"), Bytes.toBytes(reportRecord.getClicks()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("gross_impressions"), Bytes.toBytes(reportRecord.getGrossImpressions()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("impressions"), Bytes.toBytes(reportRecord.getImpressions()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("gross_view_impressions"), Bytes.toBytes(reportRecord.getGrossViewableImpressions()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("view_impressions"), Bytes.toBytes(reportRecord.getViewableImpressions()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("mobile_clicks"), Bytes.toBytes(reportRecord.getMobileClicks()));
        put.add(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("mobile_impressions"), Bytes.toBytes(reportRecord.getMobileImpressions()));
        recordList.add(new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put));
      }
      return recordList.iterator();
    }
  }
  
  /**
   * Common method to write HBase which connect Hbase to write data
   */
  public class PutDataToHase implements VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>> {
    public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> tupleIter) throws IOException {
      
      HTable transactionalTable = new HTable(TableName.valueOf(resultTable), HBaseConnection.getConnection());
      
      logger.info("---ResultTable = " + resultTable);
      Tuple2<ImmutableBytesWritable, Put> tuple = null;
      try {
        while (tupleIter.hasNext()) {
          tuple = tupleIter.next();
          transactionalTable.put(tuple._2);
        }
      } catch (IOException e) {
        logger.error(e.getMessage());
        throw e;
      } finally {
        transactionalTable.close();
      }
    }
  }
}
