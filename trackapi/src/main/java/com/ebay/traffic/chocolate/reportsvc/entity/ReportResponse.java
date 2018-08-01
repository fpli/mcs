package com.ebay.traffic.chocolate.reportsvc.entity;

import com.ebay.cos.raptor.service.annotations.ApiDescription;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ReportResponse {
  @ApiDescription("Fields from report request")
  @XmlElement(name = "requestFields")
  private ReportRequest request;

  @ApiDescription("Report records by month for publisher/campaign")
  @XmlElement(name = "reportByMonth")
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
