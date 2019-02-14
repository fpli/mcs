package com.ebay.traffic.chocolate.kafka;

import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericDatumExWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Created by yliu29 on 2/13/19.
 *
 * A serializer that is used by Kafka producer to serialize {@link RheosEvent} to byte[].
 *
 * Clone from io.ebay.rheos.schema.avro.RheosEventSerializer
 */
public class RheosEventExSerializer implements Serializer<RheosEvent> {
  private final EncoderFactory encoderFactory = EncoderFactory.get();

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
  }

  @Override
  public byte[] serialize(final String topic, final RheosEvent data) {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
      DatumWriter<GenericRecord> writer = getWriter(data);
      writer.write(data, encoder);
      encoder.flush();

      return out.toByteArray();
    } catch (Exception e) {
      throw new SerializationException("Unable to serialize common message", e);
    }
  }

  private DatumWriter<GenericRecord> getWriter(RheosEvent rheosEvent) {
    return new GenericDatumExWriter<>(rheosEvent.getSchema());
  }

  @Override
  public void close() {
  }
}
