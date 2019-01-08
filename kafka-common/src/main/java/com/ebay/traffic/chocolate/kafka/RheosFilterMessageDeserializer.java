package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by yliu29 on 2/13/18.
 *
 * The Filter Message Deserializer used in Kafka Consumer of Rheos.
 * The raw data is serialization format of rheos event. Internally it
 * skips rheos header, and deserialize the remaining data to a filter message.
 */
public class RheosFilterMessageDeserializer implements Deserializer<FilterMessage> {
  private final static Schema rheosHeaderSchema =
          RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();

  public RheosFilterMessageDeserializer() {
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public FilterMessage deserialize(String topic, byte[] data) {
    try {
      return FilterMessage.decodeRheos(rheosHeaderSchema, data);
    } catch (Exception e) {
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
