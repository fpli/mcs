package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by yliu29 on 2/12/18.
 *
 * The Listener Message Serializer used in Kafka Producer.
 */
public class ListenerMessageSerializer implements Serializer<ListenerMessage> {

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public byte[] serialize(String topic, ListenerMessage message) {
    try {
      return message.writeToBytes();
    } catch (Exception e) {
      throw new SerializationException("Unable to serialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
