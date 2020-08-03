package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by jialili1 on 8/3/20
 *
 * The Behavior Message Serializer used in Kafka Producer.
 */
public class BehaviorMessageSerializer implements Serializer<BehaviorMessage> {
  private static final Logger logger = LoggerFactory.getLogger(BehaviorMessageSerializer.class);

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public byte[] serialize(String topic, BehaviorMessage message) {
    try {
      return message.writeToBytes();
    } catch (Exception e) {
      logger.warn("Unable to serialize message");
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
