package com.ebay.traffic.chocolate.cappingrules.cassandra;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.traffic.chocolate.cappingrules.AbstractCapper;
import com.ebay.traffic.chocolate.cappingrules.IdentifierUtil;
import com.ebay.traffic.chocolate.cappingrules.constant.Env;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.cappingrules.constant.StorageType;
import com.ebay.traffic.chocolate.cappingrules.dto.FilterResultEvent;
import com.ebay.traffic.chocolate.cappingrules.dto.RawReportRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yimeng on 11/22/17.
 */
public class ReportDataGenerator extends AbstractCapper {
  
  private String storageType = StorageType.CASSANDRA.name();
  private String env = Env.QA.name();
  
  public ReportDataGenerator(String jobName, String mode, String originalTable, String resultTable, String channelType,
                             String scanStopTime, Integer scanTimeWindow, String storageType, String env) throws
      java.text.ParseException {
    super(jobName, mode, originalTable, resultTable, channelType, scanStopTime, scanTimeWindow);
    this.storageType = storageType;
    this.env = env;
  }
  
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
  
  @Override
  public void run() throws Exception {
    
    // collect data from hbase
    JavaRDD<Result> hbaseData = readFromHabse();
    
    //Get CampaignReport & PartnerReport
    JavaRDD<List<RawReportRecord>> campaignReport = getReportByReportType(hbaseData, ReportType.CAMPAIGN);
    JavaRDD<List<RawReportRecord>> partnerReport = getReportByReportType(hbaseData, ReportType.PARTNER);
    
    if (StorageType.HBASE.name().equalsIgnoreCase(storageType)) {
      //Save to HBase
      writeToHbase(campaignReport);
      writeToHbase(partnerReport);
    } else if (StorageType.CASSANDRA.name().equalsIgnoreCase(storageType)) {
      //Save to Cassandra
      writeToCassandra(campaignReport, ReportType.CAMPAIGN);
      writeToCassandra(partnerReport, ReportType.PARTNER);
    } else {
      logger().warn("Data is not stored in any persistent data base.");
    }
  }
  
  public JavaRDD<List<RawReportRecord>> getReportByReportType(JavaRDD<Result> hbaseData, ReportType reportType) {
    JavaPairRDD<Long, FilterResultEvent> campaignResultRDD = hbaseData.mapToPair(new ReadDataByReportType(reportType));
    
    JavaPairRDD<Long, Iterable<FilterResultEvent>> groupbyCampaignId = campaignResultRDD.groupByKey();
    
    JavaRDD<List<RawReportRecord>> resultRDD = groupbyCampaignId.map(new CountByReportType(reportType));
    
    return resultRDD;
  }
  
  @Override
  public <T> void writeToHbase(T writeData) {
    JavaRDD<List<RawReportRecord>> resultRDD = (JavaRDD<List<RawReportRecord>>) writeData;
    JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = resultRDD.flatMapToPair(new WriteHBaseMap());
    hbasePuts.foreachPartition(new PutDataToHase());
  }
  
  public void writeToCassandra(JavaRDD<List<RawReportRecord>> resultRDD, ReportType reportType) throws Exception {
    Iterator<List<RawReportRecord>> reportIte = resultRDD.toLocalIterator();
    
    ApplicationOptions.init("GingerClient.properties");
    ApplicationOptions applicationOptions = ApplicationOptions.getInstance();
  
    URL oauthSvcURL = CassandraService.getOauthSvcEndPoint(applicationOptions, env);
    String oauthToken = CassandraService.getOauthToken(oauthSvcURL);
    URL chocorptURL = CassandraService.getCassandraSvcEndPoint(applicationOptions, env, reportType);
    
    CassandraService cassandraService = new CassandraService();
    List<RawReportRecord> recordList = null;
    while (reportIte.hasNext()) {
      recordList = reportIte.next();
      cassandraService.saveReportRecordList(oauthToken, chocorptURL, recordList);
    }
  }
  
  @Override
  protected <T> T filterWithCapper(JavaRDD<Result> hbaseData) {
    return null;
  }
  
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
      
      int day, validRecord, mobileRecord = 0;
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
        tmpTimestamp = reportRecord.getTimestamp() == 0? Long.MAX_VALUE : reportRecord.getTimestamp();
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
        
        validRecord = (retEvent.getFilterPassed() && retEvent.isCappingPassed() && retEvent.isImpressed()) ? 1 : 0;
        mobileRecord = retEvent.getMobile() ? 1 : 0;
        
        if (ChannelAction.IMPRESSION.name().equalsIgnoreCase(retEvent.getChannelAction())) {
          reportRecord.setGrossImpressions(reportRecord.getGrossImpressions() + 1);
          reportRecord.setImpressions(reportRecord.getImpressions() + validRecord);
          reportRecord.setMobileImpressions(reportRecord.getMobileImpressions() + mobileRecord);
        } else if (ChannelAction.CLICK.name().equalsIgnoreCase(retEvent.getChannelAction())) {
          reportRecord.setGrossClicks(reportRecord.getGrossClicks() + 1);
          reportRecord.setClicks(reportRecord.getClicks() + validRecord);
          reportRecord.setMobileClicks(reportRecord.getMobileClicks() + mobileRecord);
        } else {
          reportRecord.setGrossViewableImpressions(reportRecord.getGrossViewableImpressions() + 1);
          reportRecord.setViewableImpressions(reportRecord.getViewableImpressions() + validRecord);
        }
      }
      
      return new ArrayList<RawReportRecord>(report.values());
    }
  }
  
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
}
