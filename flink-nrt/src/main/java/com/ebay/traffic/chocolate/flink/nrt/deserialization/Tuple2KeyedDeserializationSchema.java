package com.ebay.traffic.chocolate.flink.nrt.deserialization;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

public class Tuple2KeyedDeserializationSchema implements KeyedDeserializationSchema<Tuple2<Long, byte[]>> {
  @Override
  public TypeInformation getProducedType() {
    return new TupleTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
  }

  @Override
  public Tuple2<Long, byte[]> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
    long key = 0;
    for (byte b : messageKey) {
      key <<= 8;
      key |= b & 0xFF;
    }
    return new Tuple2<>(key, message);
  }

  @Override
  public boolean isEndOfStream(Tuple2<Long, byte[]> nextElement) {
    return false;
  }

}
