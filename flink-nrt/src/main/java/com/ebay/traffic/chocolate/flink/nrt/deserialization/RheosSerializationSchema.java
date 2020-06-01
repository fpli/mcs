package com.ebay.traffic.chocolate.flink.nrt.deserialization;

import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericDatumExWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;

public class RheosSerializationSchema implements KafkaSerializationSchema<Tuple3<String, Long, byte[]>> {

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
