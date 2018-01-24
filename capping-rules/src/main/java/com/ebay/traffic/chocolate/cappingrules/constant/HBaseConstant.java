package com.ebay.traffic.chocolate.cappingrules.constant;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by yimeng on 11/26/17.
 */
public class HBaseConstant {
  //hbase column family
  public static byte[] COLUMN_FAMILY_X = Bytes.toBytes("x");
  //hbase column name
  public static byte[] COL_CHANNEL_TYPE = Bytes.toBytes("channel_type");
  public static byte[] COL_CHANNEL_ACTION = Bytes.toBytes("channel_action");

  //SNID Capper Added Columns
  public static final byte[] COL_IS_IMPRESSED = Bytes.toBytes("is_impressed");
  public static final byte[] COL_IMP_ROW_KEY = Bytes.toBytes("imp_row_key");

  //IP Capper added Columns
  public static final byte[] COL_CAPPING_PASSED = Bytes.toBytes("capping_passed");
  public static final byte[] COL_CAPPING_FAILED_RULE = Bytes.toBytes("capping_failed_rule");

  //Basic Columns
  public static final  byte[] COL_SNAPSHOT_ID = Bytes.toBytes("snapshot_id");
  public static final  byte[] COL_CAMPAIGN_ID = Bytes.toBytes("campaign_id");
  public static final  byte[] COL_PARTNER_ID = Bytes.toBytes("partner_id");
  public static final  byte[] COL_REQUEST_HEADERS = Bytes.toBytes("request_headers");
  public static final  byte[] COL_REQUEST_TIMESTAMP = Bytes.toBytes("request_timestamp");
  public static final  byte[] COL_HTTP_METHOD = Bytes.toBytes("http_method");
  public static final  byte[] COL_SNID = Bytes.toBytes("snid");
  public static final  byte[] COL_MONTH = Bytes.toBytes("month");
  public static final  byte[] COL_IS_MOBILE = Bytes.toBytes("is_mobile");
  public static final  byte[] COL_FILTER_PASSED = Bytes.toBytes("filter_passed");
  public static final  byte[] COL_FILTER_FAILED_RULE = Bytes.toBytes("filter_failed_rule");

}
