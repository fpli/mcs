package com.ebay.traffic.chocolate.reportsvc.entity;

import java.util.List;

public class ReportResponse {
  // Fields from report request.
  private ReportRequest request;

  // Report records by month for publisher or campaign.
  private List<ReportRecordsPerMonth> report;

  public ReportResponse() {

  }

  public ReportResponse(ReportRequest request) {
    this.request = request;
  }

  public ReportRequest getRequest() {
    return request;
  }

  public void setRequest(ReportRequest request) {
    this.request = request;
  }

  public List<ReportRecordsPerMonth> getReport() {
    return report;
  }

  public void setReport(List<ReportRecordsPerMonth> report) {
    if (this.report == null || this.report.isEmpty()) {
      this.report = report;
    } else {
      this.report.addAll(report);
    }
  }
}
