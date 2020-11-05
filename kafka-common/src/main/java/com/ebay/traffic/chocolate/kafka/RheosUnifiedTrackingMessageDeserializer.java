package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingMessage;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by jialili1 on 11/5/20
 *
 * The Unified Tracking Message Deserializer used in Kafka Consumer of Rheos.
 * The raw data is serialization format of rheos event.
 * Internally it skips Rheos header, and deserialize the remaining data to a Unified Tracking message.
 */
public class RheosUnifiedTrackingMessageDeserializer implements Deserializer<UnifiedTrackingMessage> {
  private static final Logger logger = LoggerFactory.getLogger(RheosUnifiedTrackingMessageDeserializer.class);
  private final static Schema rheosHeaderSchema =
      RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();

  public RheosUnifiedTrackingMessageDeserializer() {
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public UnifiedTrackingMessage deserialize(String topic, byte[] data) {
    try {
      return UnifiedTrackingMessage.decodeRheos(rheosHeaderSchema, data);
    } catch (Exception e) {
      logger.warn("Unable to serialize message");
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
