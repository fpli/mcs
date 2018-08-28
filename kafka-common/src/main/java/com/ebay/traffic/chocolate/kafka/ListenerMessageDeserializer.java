package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Created by yliu29 on 2/13/18.
 *
 * The Listener Message Deserializer used in Kafka Consumer.
 */
public class ListenerMessageDeserializer implements Deserializer<ListenerMessage> {

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public ListenerMessage deserialize(String s, byte[] bytes) {
    try {
      return ListenerMessage.readFromJSON(new String(bytes));
    } catch (IOException e) {
      throw new SerializationException("Unable to deserialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
