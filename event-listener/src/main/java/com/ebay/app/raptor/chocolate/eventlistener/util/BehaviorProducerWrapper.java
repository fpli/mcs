package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaWithFallbackProducer;
import com.ebay.traffic.chocolate.kafka.RheosKafkaProducer;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jialili1 on 8/4/20
 */
public class BehaviorProducerWrapper {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BehaviorProducerWrapper.class);

  private static BehaviorProducerWrapper behaviorProducerWrapper;
  private static Producer behaviorProducer;

  /**
   * Singleton
   */
  private BehaviorProducerWrapper() {
  }

  /**
   * Initialize singleton instance
   */
  public static synchronized void init(ApplicationOptions options) {
    if (behaviorProducerWrapper == null) {
      behaviorProducerWrapper = new BehaviorProducerWrapper();
    }

    Properties kafkaProperties = options.getBehaviorKafkaProperties(KafkaCluster.KAFKA);
    Properties rheosProperties = options.getBehaviorKafkaProperties(KafkaCluster.RHEOS);

    try {
      switch (options.getSinkKafkaCluster()) {
        case "kafka":
          behaviorProducer = new KafkaProducer(kafkaProperties);
          break;
        case "rheos":
          behaviorProducer = new RheosKafkaProducer(rheosProperties);
          break;
        case "rheos" + KafkaCluster.DELIMITER + "kafka":
          behaviorProducer = new KafkaWithFallbackProducer(new RheosKafkaProducer(rheosProperties),
              new KafkaProducer(kafkaProperties), options);
          break;
        case "kafka" + KafkaCluster.DELIMITER + "rheos":
          behaviorProducer = new KafkaWithFallbackProducer(new KafkaProducer(kafkaProperties),
              new RheosKafkaProducer(rheosProperties), options);
          break;
        default:
          throw new IllegalArgumentException("Illegal kafka cluster");
      }
    } catch (Exception e) {
      logger.error("Failed to init behavior kafka producer", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Close Kafka producer.
   *
   */
  public static synchronized void close() {
    if (behaviorProducer != null) {
      behaviorProducer.close();
      behaviorProducer = null;
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
        ESMetrics.getInstance().meter("KafkaFailure");
        logger.error("Send behavior kafka error", exception);
      } else {
        logger.debug("Successfully send behavior kafka record=" + metadata);
      }
    }
  };

  public static BehaviorProducerWrapper getInstance() {
    return behaviorProducerWrapper;
  }

  public Producer getBehaviorProducer() {
    if (behaviorProducer == null) {
      throw new RuntimeException("producer has not been initialized.");
    }

    return behaviorProducer;
  }
}
