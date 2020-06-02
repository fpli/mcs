package com.ebay.traffic.chocolate.flink.nrt.deserialization;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class DefaultKafkaSerializationSchema implements KafkaSerializationSchema<Tuple3<String, Long, byte[]>> {

  @Override
  public ProducerRecord<byte[], byte[]> serialize(Tuple3<String, Long, byte[]> element, @Nullable Long timestamp) {
    Long messageKey = element.f1;
    byte[] serializedKey = new byte[]{
            (byte) (messageKey >>> 56),
            (byte) (messageKey >>> 48),
            (byte) (messageKey >>> 40),
            (byte) (messageKey >>> 32),
            (byte) (messageKey >>> 24),
            (byte) (messageKey >>> 16),
            (byte) (messageKey >>> 8),
            messageKey.byteValue()
    };
    return new ProducerRecord<>(element.f0, serializedKey, element.f2);
  }

}
