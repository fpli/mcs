package com.ebay.traffic.chocolate.reportsvc.constant;

public class Errors {
  public static final String INTERNAL_ERROR_MSG = "Internal server error encountered. If this problem persists, please contact Chocolate on-call.";
  public static final String BAD_PARTNER_MSG = "Partner id is invalid or unavailable. Please try again with valid Partner id.";
  public static final String BAD_CAMPAIGN_MSG = "Campaign id available is invalid. Please try again with valid Campaign id.";
  public static final String BAD_DATES_MSG = "Start/end date cannot be invalid if the date range is \"Custom\". Please try again with valid start/end date.";
  public static final String EXPIRED_MSG = "This request has expired. Please try again with a valid request.";
  public static final String UNAUTHORIZED_MSG = "This partner is not allowed to make this request. Please try again with a valid request.";

  public static final String ERROR_CONTENT = "chocolateReportErrorContent";
  public static final String ERROR_DOMAIN = "marketingTrackingDomain";
}
