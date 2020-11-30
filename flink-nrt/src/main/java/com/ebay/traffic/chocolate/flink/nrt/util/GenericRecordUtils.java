package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.HashMap;
import java.util.Map;

public class GenericRecordUtils {
  @SuppressWarnings("unchecked")
  public static Map<String, String> getMap(GenericRecord genericRecord, String key) {
    Map<String, String> target = new HashMap<>();
    ((Map<Utf8, Utf8>) genericRecord.get(key)).forEach((k, v) -> target.put(String.valueOf(k), String.valueOf(v)));
    return target;
  }

  public static String getStringFieldOrEmpty(GenericRecord genericRecord, String key) {
    return genericRecord.get(key) == null ? StringConstants.EMPTY : genericRecord.get(key).toString();
  }
}
