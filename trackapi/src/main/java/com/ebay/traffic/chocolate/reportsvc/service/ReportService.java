package com.ebay.traffic.chocolate.reportsvc.service;

import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportResponse;

import java.text.ParseException;

public interface ReportService {
  /**
   * Fetch data and generate report for a valid request.
   *
   * @param request Validated incoming request.
   * @return an instance of ReportResponse having mapped data for request.
   * @throws ParseException
   */
  ReportResponse generateReportForRequest(ReportRequest request) throws ParseException;
}
