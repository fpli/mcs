package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by yliu29 on 2/13/18.
 *
 * The Listener Message Deserializer used in Kafka Consumer of Rheos.
 * The raw data is serialization format of rheos event. Internally it
 * skips rheos header, and deserialize the remaining data to a listener message.
 */
public class RheosListenerMessageDeserializer implements Deserializer<ListenerMessage> {
  private static final Logger logger = Logger.getLogger(RheosListenerMessageDeserializer.class);
  private final static Schema rheosHeaderSchema =
          RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();

  public RheosListenerMessageDeserializer() {
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public ListenerMessage deserialize(String topic, byte[] data) {
    try {
      return ListenerMessage.decodeRheos(rheosHeaderSchema, data);
    } catch (Exception e) {
      logger.warn("Unable to serialize message");
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
