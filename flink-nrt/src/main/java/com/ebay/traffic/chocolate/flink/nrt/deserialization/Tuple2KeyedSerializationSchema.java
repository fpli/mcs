package com.ebay.traffic.chocolate.flink.nrt.deserialization;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class Tuple2KeyedSerializationSchema implements KeyedSerializationSchema<Tuple2<Long, byte[]>> {
  @Override
  public byte[] serializeKey(Tuple2<Long, byte[]> element) {
    Long key = element.f0;
    return new byte[]{
            (byte) (key >>> 56),
            (byte) (key >>> 48),
            (byte) (key >>> 40),
            (byte) (key >>> 32),
            (byte) (key >>> 24),
            (byte) (key >>> 16),
            (byte) (key >>> 8),
            key.byteValue()
    };
  }

  @Override
  public byte[] serializeValue(Tuple2<Long, byte[]> element) {
    return element.f1;
  }

  @Override
  public String getTargetTopic(Tuple2<Long, byte[]> element) {
    return null;
  }
}
