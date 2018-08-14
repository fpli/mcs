package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
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
public class KafkaSink {
  private static final Logger LOG = Logger.getLogger(KafkaSink.class);

  private static Producer producer;

  /**
   * Kafka Configurable Interface.
   */
  public static interface KafkaConfigurable {

    /**
     * Get the kafka cluster to sink. Refer to <code>KafkaCluster</code>
     *
     * @return Kafka cluster to sink.
     */
    String getSinkKafkaCluster();

    /**
     * Get the kafka properties to sink.
     *
     * @param sinkCluster kafka cluster
     * @return Kafka properties
     * @throws IOException
     */
    Properties getSinkKafkaProperties(KafkaCluster sinkCluster) throws IOException;
  }

  private KafkaSink() {
  }

  /**
   * Initialize the kafka sink.
   * This method can only be called once.
   *
   * @param conf the kafka configurable
   */
  public static synchronized void initialize(KafkaConfigurable conf) {
    if (producer != null) {
      throw new IllegalStateException("Can only initialize once.");
    }

    try {
      switch (conf.getSinkKafkaCluster()) {
        case "kafka":
          producer = new KafkaProducer(conf.getSinkKafkaProperties(KafkaCluster.KAFKA));
          break;
        case "rheos":
          producer = new RheosKafkaProducer(conf.getSinkKafkaProperties(KafkaCluster.RHEOS));
          break;
        case "rheos" + KafkaCluster.DELIMITER + "kafka":
          producer = new KafkaWithFallbackProducer(
                  new RheosKafkaProducer(conf.getSinkKafkaProperties(KafkaCluster.RHEOS)),
                  new KafkaProducer(conf.getSinkKafkaProperties(KafkaCluster.KAFKA)));
          break;
        case "kafka" + KafkaCluster.DELIMITER + "rheos":
          producer = new KafkaWithFallbackProducer(
                  new KafkaProducer(conf.getSinkKafkaProperties(KafkaCluster.KAFKA)),
                  new RheosKafkaProducer(conf.getSinkKafkaProperties(KafkaCluster.RHEOS)));
          break;
        default:
          throw new IllegalArgumentException("Illegal kafka cluster");
      }
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
    synchronized (KafkaSink.class) {
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
  public static void close() throws IOException {
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
        MetricsClient.getInstance().meter("KafkaFailure");
        ESMetrics.getInstance().meter("KafkaFailure", 1, System.currentTimeMillis());
        LOG.error(exception);
      } else {
        LOG.debug("Succeeded in sending kafka record=" + metadata);
      }
    }
  };
}
