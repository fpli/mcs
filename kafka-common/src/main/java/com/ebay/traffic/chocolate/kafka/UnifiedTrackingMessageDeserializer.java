package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jialili1 on 11/5/20
 *
 * The Unified Tracking Message Deserializer used in Kafka Consumer.
 */
public class UnifiedTrackingMessageDeserializer implements Deserializer<UnifiedTrackingMessage> {
  private static final Logger logger = LoggerFactory.getLogger(BehaviorMessageDeserializer.class);

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public UnifiedTrackingMessage deserialize(String s, byte[] bytes) {
    try {
      return UnifiedTrackingMessage.readFromJSON(new String(bytes));
    } catch (IOException e) {
      logger.warn("Unable to deserialize message");
      throw new SerializationException("Unable to deserialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
