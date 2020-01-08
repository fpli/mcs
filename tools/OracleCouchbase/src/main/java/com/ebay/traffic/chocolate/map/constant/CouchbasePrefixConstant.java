package com.ebay.traffic.chocolate.map.constant;

/**
 * add prefix when mapping oracle table data to couchbase
 */

public class CouchbasePrefixConstant {
  public static final String AMS_COMMON_PREFIX = "EPN_";

  public static final String AMS_TRAFFIC_SOURCE_PREFIX = AMS_COMMON_PREFIX + "ts_";

  public static final String AMS_CLICK_FILTER_TYPE_PREFIX = AMS_COMMON_PREFIX + "cft_";

  public static final String AMS_PUB_ADV_CLICK_FILTER_MAP_PREFIX = AMS_COMMON_PREFIX + "amspubfilter_";

  public static final String AMS_PROG_PUB_MAP_PREFIX = AMS_COMMON_PREFIX + "ppm_";

  public static final String AMS_PROGRAM_PREFIX = AMS_COMMON_PREFIX + "program_";

  public static final String AMS_PUBLISHER_CAMPAIGN_INFO_PREFIX = AMS_COMMON_PREFIX + "pubcmpn_";

  public static final String AMS_PUBLISHER_PREFIX = AMS_COMMON_PREFIX + "publisher_";

  public static final String AMS_PUB_DOMAIN_PREFIX = AMS_COMMON_PREFIX + "amspubdomain_";

}
