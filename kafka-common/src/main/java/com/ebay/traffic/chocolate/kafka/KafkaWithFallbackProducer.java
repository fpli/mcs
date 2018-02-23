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
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by yliu29 on 2/13/18.
 */
public class KafkaWithFallbackProducer<K, V extends GenericRecord> implements Producer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaWithFallbackProducer.class);
  private final Producer<K, V> producer1; // producer1, as primary
  private final Producer<K, V> producer2; // producer2, as fallback
  private volatile Producer<K, V> current; // current producer, should be protected in thread-safe

  private final long interval = 3 * 60 * 60 * 1000; // 3 hours
  private long time = 0;

  public KafkaWithFallbackProducer(Producer<K, V> producer1, Producer<K, V> producer2) {
    this.producer1 = producer1;
    this.producer2 = producer2;
    if (producer1 != null) {
      current = producer1;
    } else {
      current = producer2;
    }
  }

  private synchronized Producer<K, V> getCurrent() {
    if (producer1 != null && current == producer2) {
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
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    Producer<K, V> producer = getCurrent();
    try {
      return producer.send(record, callback);
    } catch (TimeoutException e) {
      // wait for "max.block.ms", if there is timeout for current producer, then switch to another producer
      LOG.warn("Send timeout", e);
      Producer<K, V> fallback = doSwitch(producer);
      if (fallback != null) {
        try {
          return fallback.send(record, callback);
        } catch (Exception e1) {
          // if there is exception while using fallback, throw it.
          LOG.error(e1.getMessage(), e1);
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public void flush() {
    try {
      getCurrent().flush();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    try {
      return getCurrent().partitionsFor(topic);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    try {
      return getCurrent().metrics();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(producer1, producer2);
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
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
