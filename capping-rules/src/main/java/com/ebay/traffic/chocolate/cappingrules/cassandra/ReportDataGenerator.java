package com.ebay.traffic.chocolate.cappingrules.cassandra;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.traffic.chocolate.cappingrules.AbstractCapper;
import com.ebay.traffic.chocolate.cappingrules.IdentifierUtil;
import com.ebay.traffic.chocolate.cappingrules.common.IStorage;
import com.ebay.traffic.chocolate.cappingrules.common.StorageFactory;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.cappingrules.constant.StorageType;
import com.ebay.traffic.chocolate.cappingrules.dto.FilterResultEvent;
import com.ebay.traffic.chocolate.report.cassandra.RawReportRecord;
import com.ebay.traffic.chocolate.report.constant.Env;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Aggregate tracking data and Generate report data into Cassandra
 * <p>
 * Created by yimeng on 11/22/17.
 */
public class ReportDataGenerator extends AbstractCapper {
  private String storageType = StorageType.CASSANDRA.name();
  private String env = Env.QA.name();

  /**
   * @param jobName       spark job name
   * @param mode          spark submit mode
   * @param originalTable HBase table which data queried from
   * @param resultTable   HBase table which data stored in
   * @param channelType   marketing channel like EPN, DAP, SEARCH
   * @param scanStopTime  scan stop time
   * @param storageType   HBASE/CASSANDRA/SMOKE_CASSANDRA
   * @param env           QA/PROD
   * @throws java.text.ParseException
   */
  public ReportDataGenerator(String jobName, String mode, String originalTable, String resultTable, String channelType,
                             String scanStopTime, Integer scanTimeWindow, String storageType, String env) throws
      java.text.ParseException {
    super(jobName, mode, originalTable, resultTable, channelType, scanStopTime, scanTimeWindow);
    this.storageType = storageType;
    this.env = env;
  }
  
  /**
   * Main function. Get parameters and then run the job.
   *
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Options options = getJobOptions("ReportDataGenerator");
    Option storageType = new Option((String) null, "storageType", true, "the storageType for ReportDataGenerator");
    storageType.setRequired(false);
    options.addOption(storageType);
    Option envOp = new Option((String) null, "env", true, "the environment for ReportDataGenerator");
    envOp.setRequired(false);
    options.addOption(envOp);
    
    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("ReportDataGenerator", options);
      System.exit(1);
      return;
    }
    
    ReportDataGenerator job = new ReportDataGenerator(cmd.getOptionValue("jobName"),
        cmd.getOptionValue("mode"), cmd.getOptionValue("originalTable"), cmd.getOptionValue("resultTable"),
        cmd.getOptionValue("channelType"), cmd.getOptionValue("scanStopTime"), Integer.valueOf(cmd.getOptionValue
        ("scanTimeWindow")), cmd.getOptionValue("storageType"), cmd.getOptionValue("env"));
    try {
      job.run();
    } finally {
      job.stop();
    }
  }
  
  /**
   * Run reportGeneratorJob
   * Step1: scan data from HBase
   * Step2: aggregate event count
   * Step3: save aggregate event count as report data to cassandra
   *
   * @throws Exception
   */
  @Override
  public void run() throws Exception {
    
    // collect data from hbase
    JavaRDD<Result> hbaseData = readFromHbase();
    
    //Get CampaignReport & PartnerReport
    JavaRDD<List<RawReportRecord>> campaignReport = getReportByReportType(hbaseData, ReportType.CAMPAIGN);
    JavaRDD<List<RawReportRecord>> partnerReport = getReportByReportType(hbaseData, ReportType.PARTNER);

    //Save CampaignReport & PartnerReport
    StorageFactory sf = new StorageFactory();
    IStorage campaignStorage = sf.getStorage(storageType);
    campaignStorage.writeToStorage(campaignReport, resultTable, env, ReportType.CAMPAIGN);
    IStorage partnerStorage = sf.getStorage(storageType);
    partnerStorage.writeToStorage(partnerReport, resultTable, env, ReportType.PARTNER);
  }
  
  /**
   * Get aggregated event count for report
   *
   * @param hbaseData  scanned data from hbase
   * @param reportType campaign/partner
   * @return
   */
  public JavaRDD<List<RawReportRecord>> getReportByReportType(JavaRDD<Result> hbaseData, ReportType reportType) {
    JavaPairRDD<Long, FilterResultEvent> campaignResultRDD = hbaseData.mapToPair(new ReadDataByReportType(reportType));
    
    JavaPairRDD<Long, Iterable<FilterResultEvent>> groupbyCampaignId = campaignResultRDD.groupByKey();
    
    JavaRDD<List<RawReportRecord>> resultRDD = groupbyCampaignId.map(new CountByReportType(reportType));
    
    return resultRDD;
  }

  /**
   * Write report data to HBase result table
   * Only work when storageType = HBase
   *
   * @param writeData aggregated report data
   * @param <T>
   */
  @Override
  public <T> void writeToHbase(T writeData) {}
  
  /**
   * Override parent method and do nothing
   *
   * @param hbaseData scanned HBase data
   * @param <T>
   * @return
   */
  @Override
  protected <T> T filterWithCapper(JavaRDD<Result> hbaseData) { return null; }

  /**
   * Group by reportType(campaign/partner)
   */
  public class ReadDataByReportType implements PairFunction<Result, Long, FilterResultEvent> {
    ReportType reportType;
    
    public ReadDataByReportType(ReportType reportType) {
      this.reportType = reportType;
    }
    
    public Tuple2<Long, FilterResultEvent> call(Result r) throws Exception {
      FilterResultEvent resultEvent = new FilterResultEvent();
      resultEvent.setRowIdentifier(r.getRow());
      resultEvent.setSnapshotId(Bytes.toLong(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("snapshot_id"))));
      resultEvent.setCampaignId(Bytes.toLong(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("campaign_id"))));
      resultEvent.setPartnerId(Bytes.toLong(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("partner_id"))));
      resultEvent.setChannelType(Bytes.toString(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("channel_type"))));
      resultEvent.setChannelAction(Bytes.toString(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("channel_action"))));
      resultEvent.setFilterPassed(Bytes.toBoolean(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("filter_passed"))));
      try {
        resultEvent.setCappingPassed(Bytes.toBoolean(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("capping_passed"))));
      } catch (NullPointerException e) {
        logger().error("There is no column named 'capping_passed'. ");
      }
      try {
        resultEvent.setImpressed(Bytes.toBoolean(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("is_impressed"))));
      } catch (NullPointerException e) {
        logger().error("There is no column named 'is_impressed'. ");
      }
      try {
        resultEvent.setMobile(Bytes.toBoolean(r.getValue(HBaseConstant.COLUMN_FAMILY_X, Bytes.toBytes("is_mobile"))));
      } catch (NullPointerException e) {
        logger().error("There is no column named 'is_mobile'. ");
      }
      
      if (ReportType.CAMPAIGN.equals(reportType)) {
        return new Tuple2<Long, FilterResultEvent>(resultEvent.getCampaignId(), resultEvent);
      } else {
        return new Tuple2<Long, FilterResultEvent>(resultEvent.getPartnerId(), resultEvent);
      }
    }
  }
  
  /**
   * Aggregate event count for different report type
   */
  public class CountByReportType implements Function<Tuple2<Long, Iterable<FilterResultEvent>>,
      List<RawReportRecord>> {
    ReportType reportType;
    
    public CountByReportType(ReportType reportType) {
      this.reportType = reportType;
    }
    
    public List<RawReportRecord> call(Tuple2<Long, Iterable<FilterResultEvent>> t)
        throws Exception {
      Iterator<FilterResultEvent> resultEventIterator = t._2.iterator();
      
      //List<RawReportRecord> campaignReportList = getReportRecord(resultEventIterator, ReportType.CAMPAIGN);
      
      HashMap<Integer, RawReportRecord> report = new HashMap<Integer, RawReportRecord>();
      
      int day, validRecordCnt, validMobileCnt, grossMobileCnt = 0;
      long snapshotId, timestamp, tmpTimestamp = Long.MAX_VALUE;
      RawReportRecord reportRecord = null;
      FilterResultEvent retEvent;
      
      while (resultEventIterator.hasNext()) {
        retEvent = resultEventIterator.next();
        snapshotId = retEvent.getSnapshotId();
        day = IdentifierUtil.getDayFromSnapshotId(snapshotId);
        if (report.get(day) == null) {
          report.put(day, new RawReportRecord());
        }
        reportRecord = report.get(day);
        
        //set 1st record timestamp/snapshotId on the same day
        timestamp = IdentifierUtil.getTimeMillisForSnapshotId(snapshotId);
        tmpTimestamp = reportRecord.getTimestamp() == 0 ? Long.MAX_VALUE : reportRecord.getTimestamp();
        if (timestamp <= tmpTimestamp) {
          reportRecord.setTimestamp(timestamp);
          reportRecord.setSnapshotId(snapshotId);
        }
        
        //set report raw data
        if (ReportType.CAMPAIGN.equals(reportType)) {
          reportRecord.setId(retEvent.getCampaignId());
        } else {
          reportRecord.setId(retEvent.getPartnerId());
        }
        reportRecord.setMonth(IdentifierUtil.getMonthFromSnapshotId(snapshotId));
        reportRecord.setDay(IdentifierUtil.getDayFromSnapshotId(snapshotId));

        validRecordCnt = (retEvent.getFilterPassed() && retEvent.isCappingPassed() && retEvent.isImpressed()) ? 1 : 0;
        validMobileCnt = retEvent.getMobile() && (retEvent.getFilterPassed() && retEvent.isCappingPassed() && retEvent.isImpressed()) ? 1 : 0;
        grossMobileCnt = retEvent.getMobile() ? 1 : 0;

        if (ChannelAction.IMPRESSION.name().equalsIgnoreCase(retEvent.getChannelAction())) {
          reportRecord.setGrossImpressions(reportRecord.getGrossImpressions() + 1);
          reportRecord.setImpressions(reportRecord.getImpressions() + validRecordCnt);
          reportRecord.setMobileImpressions(reportRecord.getMobileImpressions() + validMobileCnt);
          reportRecord.setGrossMobileImpressions(reportRecord.getGrossMobileImpressions() + grossMobileCnt);
        } else if (ChannelAction.CLICK.name().equalsIgnoreCase(retEvent.getChannelAction())) {
          reportRecord.setGrossClicks(reportRecord.getGrossClicks() + 1);
          reportRecord.setClicks(reportRecord.getClicks() + validRecordCnt);
          reportRecord.setMobileClicks(reportRecord.getMobileClicks() + validMobileCnt);
          reportRecord.setGrossMobileClicks(reportRecord.getGrossMobileClicks() + grossMobileCnt);
        } else {
          reportRecord.setGrossViewableImpressions(reportRecord.getGrossViewableImpressions() + 1);
          reportRecord.setViewableImpressions(reportRecord.getViewableImpressions() + validRecordCnt);
        }
      }
      
      return new ArrayList<RawReportRecord>(report.values());
    }
  }
}
