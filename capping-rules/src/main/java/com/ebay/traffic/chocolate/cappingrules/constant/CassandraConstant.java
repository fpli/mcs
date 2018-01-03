package com.ebay.traffic.chocolate.cappingrules.constant;

public class CassandraConstant {
  //  CONNECTION CONSTANTS
  public static final String REPORT_KEYSPACE = "chocolaterptks";
  
  //  EPN REPORT CONSTANTS
  //  COLUMN FAMILIES
  public static final String PARTNER_REPORT_CF = "partner_report";
  public static final String CAMPAIGN_REPORT_CF = "campaign_report";
  
  //  COLUMN NAMES
  
  public static final String PARTNER_ID_COLUMN = "partner_id";
  public static final String CAMPAIGN_ID_COLUMN = "campaign_id";
  public static final String MONTH_COLUMN = "month";
  public static final String DAY_COLUMN = "day";
  public static final String TIMESTAMP = "timestamp";
  public static final String SNAPSHOT_ID_COLUMN = "snapshot_id";
  
  public static final String CLICKS_COLUMN = "clicks";
  public static final String GROSS_CLICKS_COLUMN = "gross_clicks";
  public static final String IMPRESSIONS_COLUMN = "impressions";
  public static final String GROSS_IMPRESSIONS_COLUMN = "gross_impressions";
  public static final String VIEWABLE_IMPRESSIONS_COLUMN = "view_impressions";
  public static final String GROSS_VIEWABLE_IMPRESSIONS_COLUMN = "gross_view_impressions";
  public static final String MOBILE_CLICKS_COLUMN = "mobile_clicks";
  public static final String MOBILE_IMPRESSIONS_COLUMN = "mobile_impressions";
}
