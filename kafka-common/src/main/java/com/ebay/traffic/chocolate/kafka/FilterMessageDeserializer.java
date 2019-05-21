package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by yliu29 on 2/13/18.
 *
 * The Filter Message Deserializer used in Kafka Consumer
 */
public class FilterMessageDeserializer implements Deserializer<FilterMessage> {
  private static final Logger logger = Logger.getLogger(FilterMessageDeserializer.class);

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public FilterMessage deserialize(String s, byte[] bytes) {
    try {
      return FilterMessage.readFromJSON(new String(bytes));
    } catch (IOException e) {
      logger.warn("Unable to deserialize message");
      throw new SerializationException("Unable to deserialize message", e);
    }
  }

  @Override
  public void close() {
  }
}
