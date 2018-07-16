package com.ebay.traffic.chocolate.reportsvc.service;

import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportResponse;

import java.text.ParseException;

public interface ReportService {
  ReportResponse generateReportForRequest(ReportRequest request) throws ParseException;
}
