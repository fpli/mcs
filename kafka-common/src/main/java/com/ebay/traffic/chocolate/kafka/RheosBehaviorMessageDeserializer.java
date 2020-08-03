package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by jialili1 on 8/3/20
 *
 * The Behavior Message Deserializer used in Kafka Consumer of Rheos.
 * The raw data is serialization format of rheos event. Internally it
 * skips rheos header, and deserialize the remaining data to a behavior message.
 */
public class RheosBehaviorMessageDeserializer implements Deserializer<BehaviorMessage> {
  private static final Logger logger = LoggerFactory.getLogger(RheosBehaviorMessageDeserializer.class);
  private final static Schema rheosHeaderSchema =
      RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();

  public RheosBehaviorMessageDeserializer() {
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public BehaviorMessage deserialize(String topic, byte[] data) {
    try {
      return BehaviorMessage.decodeRheos(rheosHeaderSchema, data);
    } catch (Exception e) {
      logger.warn("Unable to serialize message");
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
