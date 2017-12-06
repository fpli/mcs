package com.ebay.traffic.chocolate.cappingrules.constant;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by yimeng on 11/26/17.
 */
public class HBaseConstant {
  //hbase column family
  public static byte[] COLUMN_FAMILY = Bytes.toBytes("x");
  //hbase column name
  public static byte[] COLUMN_CHANNEL_TYPE = Bytes.toBytes("channel_type");
}
