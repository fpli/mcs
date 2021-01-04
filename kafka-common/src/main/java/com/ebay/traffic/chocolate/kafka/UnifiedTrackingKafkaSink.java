package com.ebay.traffic.chocolate.kafka;

import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jialili1 on 11/24/20
 */
public class UnifiedTrackingKafkaSink {
  private static final Logger logger = LoggerFactory.getLogger(UnifiedTrackingKafkaSink.class);

  private static Producer producer;

  private UnifiedTrackingKafkaSink() {}


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
      logger.error("Failed to init kafka producer", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the producer, which is singleton.
   *
   * @return Kafka producer
   */
  public static <K, V extends GenericRecord> Producer<K, V> get() {
    synchronized (UnifiedTrackingKafkaSink.class) {
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
  public static synchronized void close() {
    if (producer != null) {
      producer.close();
      producer = null;
    }
  }

  /**
   * Callback implementation which will fail the application on failure
   */
  public static Callback callback = new Callback() {
    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method
     * will be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *            occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (metadata == null) {
        ESMetrics.getInstance().meter("UTPKafkaFailure");
        logger.error("UTP Kafka send failure.", exception);
      } else {
        logger.debug("Succeeded in sending kafka record=" + metadata);
      }
    }
  };

}
