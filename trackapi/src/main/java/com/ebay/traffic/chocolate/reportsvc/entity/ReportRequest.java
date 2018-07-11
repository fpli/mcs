package com.ebay.traffic.chocolate.reportsvc.entity;

import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import com.ebay.traffic.chocolate.reportsvc.constant.ErrorType;
import com.ebay.traffic.chocolate.reportsvc.constant.Granularity;
import com.ebay.traffic.chocolate.reportsvc.constant.ReportType;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ReportRequest {

  // Unique Id representing either partnerId or campaignId, depending on report type.
  private long id;

  // Report type can be By Campaign or By Partner.
  private ReportType reportType;

  // Predefined date range for which report is generated.
  private DateRange dateRange;

  // Granularity at which to expect report data.
  private Granularity granularity;

  // Start date for custom date range in query format - yyyyMMdd.
  private int startDate;

  // End date for custom" date range in query format - yyyyMMdd.
  private int endDate;

  // Month(s) for which report is generated in query format - yyyyMM.
  private List<Integer> months = new ArrayList<>();

  public ReportRequest() {

  }

  public ReportRequest(Map<String, String> incomingRequest) throws Exception {
    validateRequest(incomingRequest.get("partnerId"), incomingRequest.get("campaignId"));
    setIdAndReportType(incomingRequest.get("partnerId"), incomingRequest.get("campaignId"));
    setDateRangeAndStartDateAndEndDate(incomingRequest.get("dateRange"), incomingRequest.get("startDate"), incomingRequest.get("endDate"));
    calculateAndSetMonths(String.valueOf(this.startDate), String.valueOf(this.endDate));
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public ReportType getReportType() {
    return reportType;
  }

  public void setReportType(ReportType reportType) {
    this.reportType = reportType;
  }

  public DateRange getDateRange() {
    return dateRange;
  }

  public void setDateRange(DateRange dateRange) {
    this.dateRange = dateRange;
  }

  public Granularity getGranularity() {
    return granularity;
  }

  public void setGranularity(Granularity granularity) {
    this.granularity = granularity;
  }

  public int getStartDate() {
    return startDate;
  }

  public void setStartDate(int startDate) {
    this.startDate = startDate;
  }

  public int getEndDate() {
    return endDate;
  }

  public void setEndDate(int endDate) {
    this.endDate = endDate;
  }

  // Determine the type of report to be generated and the id for which to query data.
  private void setIdAndReportType(String partnerId, String campaignId) throws Exception {
    if (StringUtils.isNotEmpty(campaignId)) {
      this.id = Long.valueOf(campaignId);
      this.reportType = ReportType.CAMPAIGN;
    } else {
      if (StringUtils.isNotEmpty(partnerId)) {
        this.id = Long.valueOf(partnerId);
        this.reportType = ReportType.PARTNER;
      } else {
        throw new Exception(ErrorType.BAD_PARTNER_INFO.getErrorKey());
      }
    }
  }

  // Verify that the incoming request is in fact valid.
  private void validateRequest(String partnerId, String campaignId) throws Exception {
    if (StringUtils.isEmpty(campaignId) && StringUtils.isEmpty(partnerId)) {
      throw new Exception(ErrorType.BAD_PARTNER_INFO.getErrorKey());
    }
  }

  // Process start and end dates based on date range from request.
  private void setDateRangeAndStartDateAndEndDate(String dateRange, String startDate, String endDate) throws Exception {
    DateRange dateRangeEnum = StringUtils.isEmpty(dateRange) ? DateRange.MONTH_TO_DATE : DateRange.getDateRangeForParamName(dateRange);

    if (dateRangeEnum == DateRange.CUSTOM) {
      setDatesForCustomDateRange(startDate, endDate);
      return;
    }

    this.dateRange = dateRangeEnum;
    this.granularity = this.dateRange.getGranularity();
    String[] dates = DateRange.getDates(dateRangeEnum);
    this.startDate = Integer.valueOf(dates[0]);
    this.endDate = Integer.valueOf(dates[1]);
  }

  // Process start and end dates for custom date range.
  private void setDatesForCustomDateRange(String startDate, String endDate) throws Exception {
    if (StringUtils.isEmpty(startDate) || StringUtils.isEmpty(endDate)) {
      throw new Exception(ErrorType.BAD_START_END_DATE.getErrorKey());
    }

    this.dateRange = DateRange.CUSTOM;

    try {
      String start = DateRange.formatRequestDate(startDate);
      endDate = validateEndDate(endDate);
      String end = DateRange.formatRequestDate(endDate);

      this.startDate = Integer.valueOf(start);
      this.endDate = Integer.valueOf(end);
      this.granularity = Granularity.getGranularityForCustomDateRange(start, end);
    } catch (ParseException e) {
      throw new Exception(ErrorType.BAD_START_END_DATE.getErrorKey());
    }
  }

  // If the given end date is after today's date, return today's date.
  private static String validateEndDate(String endDate) throws ParseException {
    Date date = DateRange.REQUEST_DATE_FORMAT.parse(endDate);
    return date.after(new Date()) ? DateRange.REQUEST_DATE_FORMAT.format(new Date()) : endDate;
  }

  // Determine the months for which to query data, based on start and end dates.
  private void calculateAndSetMonths(String startDate, String endDate) throws Exception {
    try {
      this.months = DateRange.getMonthsForDateRange(startDate, endDate);
    } catch (ParseException e) {
      throw new Exception(ErrorType.BAD_START_END_DATE.getErrorKey());
    }
  }

  @Override
  public String toString() {
    return String.format(
            "Request [id: %d, reportType: %s, dateRange: %s, granularity: %s, startDate: %d, "
                    + "endDate: %d]",
            this.id,
            (this.reportType == null ? ReportType.NONE.name() : this.reportType.name()),
            (this.dateRange == null ? DateRange.MONTH_TO_DATE.name() : this.dateRange.name()),
            (this.granularity == null ? DateRange.MONTH_TO_DATE.getGranularity().name() : this.granularity.name()),
            this.startDate,
            this.endDate);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ReportRequest)) {
      return false;
    }

    ReportRequest request = (ReportRequest) obj;

    if (this.id == request.getId() &&
            this.reportType == request.getReportType() &&
            this.dateRange == request.getDateRange() &&
            this.startDate == request.getStartDate() &&
            this.endDate == request.getEndDate() &&
            this.granularity == request.getGranularity()) {
      return true;
    }
    return false;
  }
}
