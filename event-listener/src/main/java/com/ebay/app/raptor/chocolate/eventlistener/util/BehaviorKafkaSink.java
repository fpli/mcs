package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaWithFallbackProducer;
import com.ebay.traffic.chocolate.kafka.RheosKafkaProducer;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by yliu29 on 2/21/18.
 *
 * Singleton of Kafka producer. Producer is thread-safe,
 * we can get better performance by sharing one producer.
 */
public class BehaviorKafkaSink {
  private static final Logger LOG = Logger.getLogger(BehaviorKafkaSink.class);

  private static Producer producer;

  private BehaviorKafkaSink() {
  }

  /**
   * Initialize the kafka sink.
   * This method can only be called once.
   *
   * @param properties the kafka properties
   */
  public static synchronized void initialize(Properties properties) {
    if (producer != null) {
      throw new IllegalStateException("Can only initialize once.");
    }

    try {
      producer = new RheosKafkaProducer(properties);
    } catch (Exception e) {
      LOG.error("Failed to init kafka producer", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the producer, which is singleton.
   *
   * @return Kafka producer
   */
  public static <K, V extends GenericRecord> Producer<K, V> get() {
    synchronized (BehaviorKafkaSink.class) {
      if (producer == null) {
        throw new RuntimeException("producer has not been initialized.");
      }
    }
    return (Producer<K, V>) producer;
  }

  /**
   * Close Kafka producer.
   *
   * @throws IOException
   */
  public static synchronized void close() throws IOException {
    if (producer != null) {
      producer.close();
      producer = null;
    }
  }
}
