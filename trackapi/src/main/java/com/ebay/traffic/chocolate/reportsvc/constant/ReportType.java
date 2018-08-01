package com.ebay.traffic.chocolate.reportsvc.constant;

public enum ReportType {
  PARTNER, // publisher based report
  CAMPAIGN, // campaign based report
  NONE; // unknown

  ReportType() {}

  public static ReportType getByReportTypeValue(String value) {
    for (ReportType reportType : ReportType.values()) {
      if (value.equalsIgnoreCase(reportType.name())) {
        return reportType;
      }
    }
    return NONE;
  }
}
