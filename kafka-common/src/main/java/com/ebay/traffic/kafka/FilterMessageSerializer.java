package com.ebay.traffic.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Created by yliu29 on 2/12/18.
 */
public class FilterMessageSerializer implements Serializer<FilterMessage> {

  private final static DatumWriter<FilterMessage> writer = new SpecificDatumWriter<>(
          FilterMessage.getClassSchema());

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public byte[] serialize(String topic, FilterMessage message) {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(message.getClassSchema(), out);
      writer.write(message, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (Exception e) {
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
