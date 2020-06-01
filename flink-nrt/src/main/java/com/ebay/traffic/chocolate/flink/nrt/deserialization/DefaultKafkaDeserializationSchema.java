package com.ebay.traffic.chocolate.flink.nrt.deserialization;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class DefaultKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>> {
  @Override
  public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    return record;
  }

  @Override
  public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
    return TypeInformation.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() { });
  }

  @Override
  public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> nextElement) {
    return false;
  }
}
