package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.cassandra.CassandraService;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.report.cassandra.CassandraConfiguration;
import com.ebay.traffic.chocolate.report.cassandra.RawReportRecord;
import com.ebay.traffic.chocolate.report.cassandra.ReportHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Iterator;
import java.util.List;

public class CassandraStorage implements IStorage<JavaRDD<List<RawReportRecord>>> {
  private String env;
  private ReportType reportType;

  public CassandraStorage(String env, ReportType reportType) {
    this.env = env;
    this.reportType = reportType;
  }

  @Override
  public void writeToStorage(JavaRDD<List<RawReportRecord>> reportRecords) {
    reportRecords.foreachPartition(new VoidFunction<Iterator<List<RawReportRecord>>>() {
      @Override
      public void call(Iterator<List<RawReportRecord>> reportIte) throws Exception {
        if (reportIte == null) return;
        ReportHelper reportHelper = ReportHelper.getInstance();
        CassandraConfiguration cassandraConf = CassandraConfiguration.createConfiguration(env);
        reportHelper.connectToCassandra(cassandraConf);
        List<RawReportRecord> recordList = null;
        while (reportIte.hasNext()) {
          recordList = reportIte.next();
          for (RawReportRecord reportRecord : recordList) {
            if (ReportType.CAMPAIGN.equals(reportType)) {
              reportHelper.saveCampaignReport(reportRecord);
            } else {
              reportHelper.savePartnerReport(reportRecord);
            }
          }
        }
      }
    });
  }

  /**
   * Write data to Cassandra which call chocolate report service to write data
   */
  @Deprecated
  public class SaveDataToCassandraByService implements VoidFunction<Iterator<List<RawReportRecord>>> {
    private String oauthToken;
    private URL chocorptSvcURL;

    public SaveDataToCassandraByService(String oauthToken, URL chocorptSvcURL) {
      this.oauthToken = oauthToken;
      this.chocorptSvcURL = chocorptSvcURL;
    }

    public void call(Iterator<List<RawReportRecord>> reportIte) throws Exception {

      CassandraService cassandraService = CassandraService.getInstance();
      List<RawReportRecord> recordList = null;
      while (reportIte.hasNext()) {
        recordList = reportIte.next();
        cassandraService.saveReportRecordList(oauthToken, chocorptSvcURL, recordList);
      }
    }
  }

}
