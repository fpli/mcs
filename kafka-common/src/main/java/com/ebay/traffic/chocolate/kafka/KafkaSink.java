package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
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
 * Singleton of Kafka producer.
 */
public class KafkaSink {
  private static final Logger LOG = Logger.getLogger(KafkaSink.class);

  private static Producer producer;

  public static interface KafkaConfigurable {
    String getKafkaCluster();

    Properties getKafkaProperties() throws IOException;

    Properties getRheosKafkaProperties() throws IOException;
  }

  private KafkaSink() {
  }

  public static synchronized void initialize(KafkaConfigurable conf) {
    if (producer != null) {
      throw new IllegalStateException("Can only initialize once.");
    }

    try {
      switch (conf.getKafkaCluster()) {
        case "kafka":
          producer = new KafkaProducer(conf.getKafkaProperties());
          break;
        case "rheos":
          producer = new RheosKafkaProducer(conf.getRheosKafkaProperties());
          break;
        case "rheos,kafka":
          producer = new KafkaWithFallbackProducer(new RheosKafkaProducer(conf.getRheosKafkaProperties()),
                  new KafkaProducer(conf.getKafkaProperties()));
          break;
        case "kafka,rheos":
          producer = new KafkaWithFallbackProducer(
                  new KafkaProducer(conf.getKafkaProperties()),
                  new RheosKafkaProducer(conf.getRheosKafkaProperties()));
          break;
        default:
          throw new IllegalArgumentException("Illegal kafka cluster");
      }
    } catch (Exception e) {
      LOG.error("Failed to init kafka producer", e);
      throw new RuntimeException(e);
    }
  }

  public static <K, V extends GenericRecord> Producer<K, V> get() {
    synchronized (KafkaSink.class) {
      if (producer == null) {
        throw new RuntimeException("producer has not been initialized.");
      }
    }
    return (Producer<K, V>) producer;
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
        LOG.error(exception);
      } else {
        LOG.debug("Succeeded in sending kafka record=" + metadata);
      }
    }
  };
}
