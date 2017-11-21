package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.BaseSparkJob;
import com.google.protobuf.ServiceException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yimeng on 11/12/17.
 */
public abstract class AbstractCapper extends BaseSparkJob {
  protected static final String INPUT_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
  protected static short MOD = 293;
  protected final byte[] columnFamily = Bytes.toBytes("x");
  protected final String originalTable;
  protected final String resultTable;
  protected final String startTime;
  protected final String stopTime;
  ;
  
  public AbstractCapper(String jobName, String mode, String originalTable, String resultTable, String startTime,
                        String stopTime) {
    super(jobName, mode, false);
    this.originalTable = originalTable;
    this.resultTable = resultTable;
    this.startTime = startTime;
    this.stopTime = stopTime;
    ;
  }
  
  public static Options getJobOptions(String cappingRuleDescription) {
    Options options = new Options();
    Option jobName = new Option((String) null, "jobName", true, "The job name");
    options.addOption(jobName);
    
    Option mode = new Option((String) null, "mode", true, "spark on yarn or local");
    mode.setRequired(true);
    options.addOption(mode);
    
    Option originalTable = new Option((String) null, "originalTable", true, "originalTable read from HBase");
    originalTable.setRequired(true);
    options.addOption(originalTable);
    
    Option resultTable = new Option((String) null, "resultTable", true, "resultTable write to HBase");
    resultTable.setRequired(true);
    options.addOption(resultTable);
    
    Option startTime = new Option((String) null, "startTime", true, "the startTime for " + cappingRuleDescription);
    startTime.setRequired(true);
    options.addOption(startTime);
    
    Option endTime = new Option((String) null, "endTime", true, "the endTime for " + cappingRuleDescription);
    endTime.setRequired(true);
    options.addOption(endTime);
    
    return options;
  }
  
  /**
   * Read Data with data range from hbase
   *
   * @return hbase scan result
   */
  protected JavaRDD<Result> readFromHabse() throws IOException, ServiceException, ParseException {
    List<Integer> slices = new ArrayList<Integer>(MOD);
    for (int i = 0; i < MOD; i++) {
      slices.add(i);
    }
    
    SimpleDateFormat sdf = new SimpleDateFormat(INPUT_DATE_FORMAT);
    final long startTimestamp = sdf.parse(startTime).getTime();
    final long stopTimestamp = sdf.parse(stopTime).getTime();
    logger().info("originalTable = " + originalTable);
    logger().info("startTimestamp = " + startTimestamp);
    logger().info("stopTimestamp = " + stopTimestamp);
    
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
              logger().info("HBase is running!");
            } catch
                (MasterNotRunningException e) {
              logger().error("HBase is not running!");
              logger().error(e.getMessage());
              throw new MasterNotRunningException(e);
            } catch (Exception ce) {
              logger().error("Unexpected exception when check HBase!");
              logger().error(ce.getMessage());
              throw new Exception(ce);
            }
            
            byte[] startRowKey = IdentifierUtil.generateIdentifier(startTimestamp, 0, slice.shortValue());
            byte[] stopRowKey = IdentifierUtil.generateIdentifier(stopTimestamp, 0, slice.shortValue());
            
            HBaseScanIterator hBaseScanIterator = new HBaseScanIterator(originalTable, startRowKey, stopRowKey);
            return hBaseScanIterator;
          }
        });
    return javaRDD;
  }
  
  protected abstract <T> T filterWithCapper(JavaRDD<Result> hbaseData);
  
  public abstract <T> void writeToHbase(T writeData, String table);
  
  public class PutDataToHase implements VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Put>>> {
    public void call(Iterator<Tuple2<ImmutableBytesWritable, Put>> tupleIter) throws IOException {
      
      HTable transactionalTable = new HTable(TableName.valueOf(resultTable), HBaseConnection.getConnection());
      
      logger().info("---ResultTable = " + resultTable);
      Tuple2<ImmutableBytesWritable, Put> tuple = null;
      try {
        while (tupleIter.hasNext()) {
          tuple = tupleIter.next();
          transactionalTable.put(tuple._2);
        }
      } catch (IOException e) {
        logger().error(e.getMessage());
        throw e;
      }finally {
        transactionalTable.close();
      }
    }
  }
}
