package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by yliu29 on 2/12/18.
 *
 * The Filter Message Serializer used in kafka producer
 */
public class FilterMessageSerializer implements Serializer<FilterMessage> {

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public byte[] serialize(String topic, FilterMessage message) {
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
