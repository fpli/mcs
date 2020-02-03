package com.ebay.traffic.chocolate.map.constant;

/**
 * oracle table query
 */

public class OracleQueryConstant {
  public static final String AMS_TRAFFIC_SOURCE_SQL = "select * from AMS_TRAFFIC_SOURCE";

  public static final String AMS_CLICK_FILTER_TYPE_SQL = "select * from AMS_CLICK_FILTER_TYPE";

  public static final String AMS_PUB_ADV_CLICK_FILTER_MAP_SQL = "select * from AMS_PUB_ADV_CLICK_FILTER_MAP where AMS_PUBLISHER_ID in ( select distinct AMS_PUBLISHER_ID from AMS_PUB_ADV_CLICK_FILTER_MAP where last_modified_date between to_date('%s','yyyy-mm-dd hh24:mi:ss') and to_date('%s','yyyy-mm-dd hh24:mi:ss') )";

  public static final String AMS_PROG_PUB_MAP_SQL = "select AMS_PROGRAM_ID, AMS_PUBLISHER_ID, STATUS_ENUM, REJECT_REASON_ENUM from AMS_PROG_PUB_MAP where last_modified_date between to_date('%s','yyyy-mm-dd hh24:mi:ss') and to_date('%s','yyyy-mm-dd hh24:mi:ss')";

  public static final String AMS_PROGRAM_SQL = "select AMS_PROGRAM_ID, AMS_COUNTRY_ID, PROGRAM_NAME from AMS_PROGRAM";

  public static final String AMS_PUBLISHER_CAMPAIGN_INFO_SQL = "select AMS_PUBLISHER_CAMPAIGN_ID, AMS_PUBLISHER_ID, PUBLISHER_CAMPAIGN_NAME, STATUS_ENUM from AMS_PUBLISHER_CAMPAIGN where last_modified_date between to_date('%s','yyyy-mm-dd hh24:mi:ss') and to_date('%s','yyyy-mm-dd hh24:mi:ss')";

  public static final String AMS_PUBLISHER_SQL = "select AMS_PUBLISHER_ID, APPLICATION_STATUS_ENUM, BIZTYPE_ENUM, AMS_PUBLISHER_BIZMODEL_ID, AMS_CURRENCY_ID, AMS_COUNTRY_ID from AMS_PUBLISHER where last_modified_date between to_date('%s','yyyy-mm-dd hh24:mi:ss') and to_date('%s','yyyy-mm-dd hh24:mi:ss')";

  public static final String AMS_PUB_DOMAIN_SQL = "select AMS_PUB_DOMAIN_ID, AMS_PUBLISHER_ID, VALIDATION_STATUS_ENUM, URL_DOMAIN, DOMAIN_TYPE_ENUM, WHITELIST_STATUS_ENUM, DOMAIN_STATUS_ENUM, FULL_URL, PUBLISHER_BIZMODEL_ID, PUBLISHER_CATEGORY_ID, IS_REGISTERED from AMS_PUB_DOMAIN where AMS_PUBLISHER_ID in ( select distinct AMS_PUBLISHER_ID from AMS_PUB_DOMAIN where last_modified_date between to_date('%s','yyyy-mm-dd hh24:mi:ss') and to_date('%s','yyyy-mm-dd hh24:mi:ss') )";

  public static final String AMS_PUB_ADV_CLICK_FILTER_MAP_ALL_DAYS_SQL = "select count(*) from AMS_PUB_ADV_CLICK_FILTER_MAP";

  public static final String AMS_PROG_PUB_MAP_ALL_DAYS_SQL = "select count(*) from AMS_PROG_PUB_MAP";

  public static final String AMS_PUBLISHER_CAMPAIGN_INFO_ALL_DAYS_SQL = "select count(*) from AMS_PUBLISHER_CAMPAIGN";

  public static final String AMS_PUBLISHER_ALL_DAYS_SQL = "select count(*) from AMS_PUBLISHER";

  public static final String AMS_PUB_DOMAIN_ALL_DAYS_SQL = "select count(*) from AMS_PUB_DOMAIN";

}
