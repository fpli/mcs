package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jialili1 on 8/3/20
 *
 * The Behavior Message Deserializer used in Kafka Consumer.
 */
public class BehaviorMessageDeserializer implements Deserializer<BehaviorMessage> {
  private static final Logger logger = LoggerFactory.getLogger(BehaviorMessageDeserializer.class);

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public BehaviorMessage deserialize(String s, byte[] bytes) {
    try {
      return BehaviorMessage.readFromJSON(new String(bytes));
    } catch (IOException e) {
      logger.warn("Unable to deserialize message");
      throw new SerializationException("Unable to deserialize message", e);
    }
  }

  @Override
  public void close() {
  }

}
