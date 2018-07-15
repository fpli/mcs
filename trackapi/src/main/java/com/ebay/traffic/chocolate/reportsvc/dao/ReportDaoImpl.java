package com.ebay.traffic.chocolate.reportsvc.dao;

import com.couchbase.client.java.Bucket;
import com.ebay.traffic.chocolate.mkttracksvc.dao.CouchbaseClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ReportDaoImpl implements ReportDao {

  private Bucket bucket;

  @Autowired
  public ReportDaoImpl(CouchbaseClient client) {
    this.bucket = client.getReportBucket();
  }

  public List<ReportDo> getAllDataForMonth() {
    return null;
  }

  public List<ReportDo> getAllDataForDate() {
    return null;
  }

  public List<ReportDo> getAllDataForDateRange() {
    return null;
  }

  public List<ReportDo> getAllDataGreaterThanDate() {
    return null;
  }

  public List<ReportDo> getAllDataLessThanDate() {
    return null;
  }
}
