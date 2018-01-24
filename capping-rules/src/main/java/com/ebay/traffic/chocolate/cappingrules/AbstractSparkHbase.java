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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Abstract Capper for all Capping rules
 * <p>
 * Created by yimeng on 11/12/17.
 */
public abstract class AbstractSparkHbase extends BaseSparkJob {
  //hbase prefix of row identifier
  protected static short MOD = 293;
  //spark job input parameter
  protected final String originalTable, resultTable, channelType, scanStopTime;
  //update HBase data window start time
  protected long scanTimeWindowStartTime, scanTimeWindowStopTime, updateWindowStartTime = 0;
  
  /**
   * Constructor for Capping Rule with updateTimeWindow
   *
   * @param jobName        spark job name
   * @param mode           spark submit mode
   * @param originalTable  HBase table which data queried from
   * @param resultTable    HBase table which data stored in
   * @param channelType    marketing channel like EPN, DAP, SEARCH
   * @param scanStopTime   scan stop time
   * @param scanTimeWindow scan time window (minutes)
   */
  public AbstractSparkHbase(String jobName, String mode, String originalTable, String resultTable, String channelType,
                            String scanStopTime, Integer scanTimeWindow) throws ParseException {
    this(jobName, mode, originalTable, resultTable, channelType, scanStopTime, scanTimeWindow, 0);
  }
  
  /**
   * Constructor for Capping Rule with updateTimeWindow
   *
   * @param jobName          spark job name
   * @param mode             spark submit mode
   * @param originalTable    HBase table which data queried from
   * @param resultTable      HBase table which data stored in
   * @param channelType      marketing channel like EPN, DAP, SEARCH
   * @param scanStopTime     scan stop time
   * @param scanTimeWindow   scan time window (minutes)
   * @param updateTimeWindow HBase data update time window (minutes)
   */
  public AbstractSparkHbase(String jobName, String mode, String originalTable, String resultTable, String channelType,
                            String scanStopTime, Integer scanTimeWindow, Integer updateTimeWindow) throws ParseException {
    super(jobName, mode, false);
    this.originalTable = originalTable;
    this.resultTable = resultTable;
    this.channelType = channelType;
    this.scanStopTime = scanStopTime;
    logger().info("=======hbase originalTable ======" + originalTable);
    logger().info("=======hbase resultTable ======" + resultTable);
    logger().info("=======channelType ======" + channelType);
    logger().info("=======hbase scanStopTime ======" + scanStopTime);
    logger().info("=======hbase scanTimeWindow ======" + scanTimeWindow);
    logger().info("=======hbase updateTimeWindow ======" + updateTimeWindow);
//    scanTimeWindowStopTime = new SimpleDateFormat(INPUT_DATE_FORMAT).parse(scanStopTime).getTime();
    scanTimeWindowStopTime = IdentifierUtil.INPUT_DATE_FORMAT.parse(scanStopTime).getTime();
    scanTimeWindowStartTime = scanTimeWindowStopTime - scanTimeWindow * 60 * 1000;
    if (updateTimeWindow > 0) {
      updateWindowStartTime = scanTimeWindowStopTime - updateTimeWindow * 60 * 1000;
    }
  }
  
  /**
   * Get job running parameters
   *
   * @param cappingRuleDescription capping rule descriptions
   * @return running params
   */
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
    resultTable.setRequired(false);
    options.addOption(resultTable);
    
    Option channelType = new Option((String) null, "channelType", true, "the channelType for " + cappingRuleDescription);
    channelType.setRequired(true);
    options.addOption(channelType);
    
    Option scanStopTime = new Option((String) null, "scanStopTime", true, "the scanStopTime for " + cappingRuleDescription);
    scanStopTime.setRequired(true);
    options.addOption(scanStopTime);
    
    Option scanTimeWindow = new Option((String) null, "scanTimeWindow", true, "the scanTimeWindow for " + cappingRuleDescription);
    scanTimeWindow.setRequired(true);
    options.addOption(scanTimeWindow);
    
    Option updateTimeWindow = new Option((String) null, "updateTimeWindow", true, "the timeWindowMinutes for " +
        cappingRuleDescription);
    updateTimeWindow.setRequired(false);
    options.addOption(updateTimeWindow);
    
    return options;
  }
  
  /**
   * Read Data with data range from hbase
   *
   * @return hbase scan result
   */
  protected JavaRDD<Result> readFromHbase() throws IOException, ServiceException, ParseException {
    List<Integer> slices = new ArrayList<Integer>(MOD);
    for (int i = 0; i < MOD; i++) {
      slices.add(i);
    }

    logger().info("originalTable = " + originalTable);
    logger().info("startTimestamp = " + scanTimeWindowStartTime);
    logger().info("stopTimestamp = " + scanTimeWindowStopTime);
    
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
            
            byte[] startRowKey = IdentifierUtil.generateIdentifier(scanTimeWindowStartTime, 0, slice.shortValue());
            byte[] stopRowKey = IdentifierUtil.generateIdentifier(scanTimeWindowStopTime, 0, slice.shortValue());
            
            HBaseScanIterator hBaseScanIterator = new HBaseScanIterator(originalTable, startRowKey, stopRowKey, channelType);
            return hBaseScanIterator;
          }
        });
    return javaRDD;
  }
  
  /**
   * Abstract method to filter data by specific capping rules
   *
   * @param hbaseData scanned HBase data
   * @return filtered data
   */
  protected abstract <T> T filterWithCapper(JavaRDD<Result> hbaseData);
  
  /**
   * Abstract method to write data back to HBase by specific capping rules
   *
   * @param writeData filtered data
   */
  public abstract <T> void writeToHbase(T writeData);
  
  /**
   * Common method to write HBase which connect Hbase to write data
   */
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
      } finally {
        transactionalTable.close();
      }
    }
  }
}
