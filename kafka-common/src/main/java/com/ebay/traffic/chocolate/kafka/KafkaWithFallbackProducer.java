package com.ebay.traffic.chocolate.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by yliu29 on 2/13/18.
 *
 * This producer is constructed by two producers, one is primary producer, another
 * is fallback producer. Internally if there is issue for the primary producer, such
 * as the Kafka is outage, it switches to use the fallback producer.
 *
 * ### Unstable ###
 */
public class KafkaWithFallbackProducer<K, V extends GenericRecord> implements Producer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaWithFallbackProducer.class);
  private final Producer<K, V> producer1; // producer1, as primary
  private final Producer<K, V> producer2; // producer2, as fallback
  private volatile Producer<K, V> current; // current producer, should be protected in thread-safe

  private final long interval = 3 * 60 * 60 * 1000; // 3 hours
  private long time = 0;

  public KafkaWithFallbackProducer(Producer<K, V> producer1, Producer<K, V> producer2) {
    assert producer1 != null;
    this.producer1 = producer1;
    this.producer2 = producer2;
    this.current = producer1;
  }

  private synchronized Producer<K, V> getCurrent() {
    if (current == producer2) {
      // we are using the fallback producer, need to check whether primary kafka is back after "interval".
      long curr = System.currentTimeMillis();
      if (curr - time > interval) {
        try {
          current.flush(); // flush current producer
        } catch (Exception e) {
          LOG.warn(e.getMessage(), e);
        }
        // try to switch to producer1 after "interval".
        LOG.info("Try to switch to producer1 after \"interval\".");
        current = producer1;
      }
    }
    return current;
  }

  private synchronized Producer<K, V> doSwitch(Producer<K, V> using) {
    // only if we have two producers
    if (producer1 == null || producer2 == null) {
      return null;
    }
    // if the using producer is same as the current producer
    if (using == current) {
      if (current == producer1) {
        current = producer2; // switch to fallback producer
        time = System.currentTimeMillis(); // mark the time we start to use fallback producer
        LOG.info("Switch to fallback producer.");
      } else {
        current = producer1; // switch to primary producer
        LOG.info("Switch to primary producer.");
      }
    }

    return current;
  }

  @Override
  public synchronized Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  @Override
  public synchronized Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    String topics = record.topic();
    String[] topicarray = topics.split(KafkaCluster.DELIMITER);
    String topic1 = topicarray[0];
    String topic2 = topicarray.length > 1 ? topicarray[1] : topic1;

    Producer<K, V> producer = getCurrent();
    String topic = producer == producer1 ? topic1 : topic2;
    ProducerRecord<K, V> pr = new ProducerRecord<>(topic, record.key(), record.value());

    Callback cb = (recordMetadata, e) -> {

      if (e != null && e instanceof TimeoutException) {
        // Currently TimeoutException happens in two cases: 1. Failed to update metadata after "max.block.ms", 2.
        // Block "buffer.memory" is full and can't get space in "max.block.ms". Both these two cases will block
        // current thread.
        // wait for "max.block.ms", if there is timeout for current producer, then switch to another producer
        LOG.warn("Send timeout", e);

        Producer<K, V> fallback = doSwitch(producer);
        if (fallback != null) {
          String ftopic = fallback == producer1 ? topic1 : topic2;
          ProducerRecord<K, V> fpr = new ProducerRecord<>(ftopic, record.key(), record.value());
          fallback.send(fpr, callback);
        }
      }
      callback.onCompletion(recordMetadata, e);
    };

    return producer.send(pr, cb);
  }

  @Override
  public synchronized void flush() {
    try {
      getCurrent().flush();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public synchronized List<PartitionInfo> partitionsFor(String topic) {
    try {
      return getCurrent().partitionsFor(topic);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  @Override
  public synchronized Map<MetricName, ? extends Metric> metrics() {
    try {
      return getCurrent().metrics();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  @Override
  public synchronized void close() {
    IOUtils.closeQuietly(producer1, producer2);
  }

  @Override
  public synchronized void close(long timeout, TimeUnit timeUnit) {
    try {
      producer1.close(timeout, timeUnit);
    } catch (Exception e) {
    }

    try {
      producer2.close(timeout, timeUnit);
    } catch (Exception e) {
    }
  }
}
