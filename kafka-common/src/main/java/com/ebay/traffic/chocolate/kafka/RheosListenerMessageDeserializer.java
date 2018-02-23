package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by yliu29 on 2/13/18.
 */
public class RheosListenerMessageDeserializer implements Deserializer<ListenerMessage> {
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private final DatumReader<GenericRecord> rheosHeaderReader;
  private final DatumReader<ListenerMessage> reader;

  public RheosListenerMessageDeserializer() {
    rheosHeaderReader = new GenericDatumReader<>(
            RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema());
    reader = new SpecificDatumReader<>(ListenerMessage.getClassSchema());
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public ListenerMessage deserialize(String topic, byte[] data) {
    try {
      BinaryDecoder decoder = decoderFactory.binaryDecoder(data, null);
      rheosHeaderReader.read(null, decoder);

      ListenerMessage message = new ListenerMessage();
      message = reader.read(message, decoder);
      return message;
    } catch (Exception e) {
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
