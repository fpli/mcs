package com.ebay.traffic.chocolate.cappingrules.hdfs;


import com.ebay.traffic.chocolate.cappingrules.AbstractSparkHbase;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import com.ebay.traffic.chocolate.cappingrules.dto.EventSchema;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 * Dump hbase data to hdfs parquet files
 * <p>
 * Created by yimeng on 01/18/18.
 */
public class HDFSFileGenerator extends AbstractSparkHbase {
  private String filePath;
  private Integer numberOfPartition = 0;

  /**
   * Constructor for HDFS Parquet File Generator
   *
   * @param jobName           spark job name
   * @param mode              spark submit mode
   * @param originalTable     HBase table which data queried from
   * @param channelType       marketing channel like EPN, DAP, SEARCH
   * @param scanStopTime      scan stop time
   * @param scanTimeWindow    scan time window (minutes)
   * @param filePath          hdfs file store path
   * @param numberOfPartition default=0 will use hbase scan partition as default partition like 293, but we could change it by this param
   * @throws java.text.ParseException
   */
  public HDFSFileGenerator(String jobName, String mode, String originalTable, String channelType, String
      scanStopTime, int scanTimeWindow, String filePath, Integer numberOfPartition) throws java.text.ParseException {
    super(jobName, mode, originalTable, null, channelType, scanStopTime, scanTimeWindow);
    this.filePath = filePath;
    this.numberOfPartition = numberOfPartition;
  }

  /**
   * Main function. Get parameters and then run the job.
   *
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Options options = getJobOptions("HDFSFileGenerator Job");
    Option filePathOp = new Option((String) null, "filePath", true, "the filePath for HDFSFileGenerator Job");
    filePathOp.setRequired(true);
    options.addOption(filePathOp);

    Option numberOfPartitionOp = new Option((String) null, "numberOfPartition", true, "the numberOfPartition for HDFSFileGenerator Job");
    numberOfPartitionOp.setRequired(false);
    options.addOption(numberOfPartitionOp);

    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("HDFSFileGeneratorJob", options);
      System.exit(1);
      return;
    }

    HDFSFileGenerator job = new HDFSFileGenerator(cmd.getOptionValue("jobName"),
        cmd.getOptionValue("mode"), cmd.getOptionValue("originalTable"), cmd.getOptionValue("channelType"),
        cmd.getOptionValue("scanStopTime"), Integer.valueOf(cmd.getOptionValue("scanTimeWindow")), cmd.getOptionValue("filePath"),
        Integer.valueOf(cmd.getOptionValue("numberOfPartition")));
    try {
      job.run();
    } finally {
      job.stop();
    }
  }

  /**
   * Run SNID Capping Rule
   * step1 : scan data from HBase
   * step2: dump data to parquet files
   *
   * @throws Exception job runtime exception
   */
  @Override
  public void run() throws Exception {
    // scan data from HBase
    JavaRDD<Result> hbaseData = readFromHbase();

    JavaRDD<EventSchema> rowRDD = hbaseData.map(new ReadDataFromHase());
    if(numberOfPartition > 0){
      rowRDD = rowRDD.repartition(numberOfPartition);
    }
    Dataset<Row> schemaDS = sqlsc().createDataFrame(rowRDD, EventSchema.class);

    // save file as parquet format
    schemaDS.write()
        //.option("compression", "gzip")
        .parquet(filePath + scanStopTime + "-" + channelType);
  }

  /**
   * transform sessionId as the row key instead of default row identifier
   */
  public class ReadDataFromHase implements Function<Result, EventSchema> {
    public EventSchema call(Result entry) throws Exception {
      EventSchema eventSchema = new EventSchema();
      eventSchema.setRowIdentifier(entry.getRow());

      eventSchema.setSnapshotId(Bytes.toLong(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_SNAPSHOT_ID)));
      eventSchema.setCampaignId(Bytes.toLong(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_CAMPAIGN_ID)));
      eventSchema.setPartnerId(Bytes.toLong(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_PARTNER_ID)));
      eventSchema.setChannelType(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_CHANNEL_TYPE)));
      eventSchema.setChannelAction(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_CHANNEL_ACTION)));
      eventSchema.setRequestHeaders(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_REQUEST_HEADERS)));
      eventSchema.setRequestTimestamp(Bytes.toLong(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_REQUEST_TIMESTAMP)));
      eventSchema.setHttpMethod(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_HTTP_METHOD)));
      eventSchema.setMonth(Bytes.toInt(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_MONTH)));
      eventSchema.setFilterPassed(Bytes.toBoolean(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_FILTER_PASSED)));
      eventSchema.setFilterFailedRule(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_FILTER_FAILED_RULE)));

      try{
        eventSchema.setSnid(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_SNID)));
      } catch (NullPointerException e){
        logger().warn("There is no column named 'snid'. ");
      }
      try {
        eventSchema.setMobile(Bytes.toBoolean(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_IS_MOBILE)));
      } catch (NullPointerException e) {
        logger().warn("There is no column named 'is_mobile'. ");
      }
      try {
        eventSchema.setImpressed(Bytes.toBoolean(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_IS_IMPRESSED)));
      } catch (NullPointerException e) {
        logger().warn("There is no column named 'is_impressed'. ");
      }
      try {
        eventSchema.setImpRowIdentifier(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_IMP_ROW_KEY));
      } catch (NullPointerException e) {
        logger().info("There is no column named 'imp_row_identifier'. ");
      }
      try {
        eventSchema.setCappingFailedRule(Bytes.toString(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_CAPPING_FAILED_RULE)));
      } catch (NullPointerException e) {
        logger().warn("There is no column named 'capping_failed_rule'. ");
      }
      try {
        eventSchema.setCappingPassed(Bytes.toBoolean(entry.getValue(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_CAPPING_PASSED)));
      } catch (NullPointerException e) {
        logger().warn("There is no column named 'capping_passed'. ");
      }
      return eventSchema;
    }
  }

  /**
   * For Unit Testing
   * @param filePath parquet file path
   * @return
   */
  public Dataset<EventSchema> readFromParquet(String filePath){
    String fileName = filePath + scanStopTime + "-" + channelType;
    return sqlsc().read().parquet(fileName).as(Encoders.bean(EventSchema.class));
  }

  @Override
  protected <T> T filterWithCapper(JavaRDD<Result> hbaseData) {
    return null;
  }

  @Override
  public <T> void writeToHbase(T writeData) {

  }
}
