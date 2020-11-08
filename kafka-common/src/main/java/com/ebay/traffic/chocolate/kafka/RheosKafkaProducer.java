package com.ebay.traffic.chocolate.kafka;

import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.ebay.traffic.chocolate.kafka.RheosConstants.*;

/**
 * Created by yliu29 on 2/11/18.
 *
 * Rheos Kafka Producer, it wraps normal avro schema format to Rheos schema format,
 * which contains an additional Rheos Header.
 *
 * This producer accepts normal avro format message, like <code>ListenerMessage</code>,
 * <code>FilterMessage</code>, internally it sends the message as a rheos event.
 */
public class RheosKafkaProducer<K, V extends GenericRecord> implements Producer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(RheosKafkaProducer.class);
  private final Producer<K, RheosEvent> producer;
  private final int schemaId;
  private final Schema schema;
  private final String producerName;

  public RheosKafkaProducer(Properties properties) {
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RheosEventExSerializer.class.getName());
    producer = new KafkaProducer<>(properties);

    Map<String, Object> map = new HashMap<>();
    Object serviceUrlDefault = properties.get(RHEOS_SERVICES_URL_KEY);
    if (serviceUrlDefault == null) {
      serviceUrlDefault = RHEOS_SERVICE_URL_DEFAULT;
    }
    map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, serviceUrlDefault);

    SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
            new SchemaRegistryAwareAvroSerializerHelper<>(map, GenericRecord.class);

    String schemaName = (String)properties.get(RHEOS_TOPIC_SCHEMA_KEY);
    if (schemaName == null) {
      throw new IllegalArgumentException("Miss " + RHEOS_TOPIC_SCHEMA_KEY);
    }
    schemaId = serializerHelper.getSchemaId(schemaName);
    schema = serializerHelper.getSchema(schemaName);
    producerName = (String)properties.get(RHEOS_PRODUCER_KEY);
    if (producerName == null) {
      throw new IllegalArgumentException("Miss " + RHEOS_PRODUCER_KEY);
    }
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    return producer.send(new ProducerRecord<>(record.topic(),
            record.key(), getRheosEvent(record.value())), callback);
  }

  /**
   * Wraps normal avro schema format to Rheos schema format,
   * which contains an additional Rheos Header.
   * @param v normal avro format message
   * @return rheos event
   */
  public RheosEvent getRheosEvent(V v) {
    RheosEvent rheosEvent = new RheosEvent(schema);
    long t = System.currentTimeMillis();
    rheosEvent.setEventCreateTimestamp(t);
    rheosEvent.setEventSentTimestamp(t);
    rheosEvent.setProducerId(producerName);
    rheosEvent.setSchemaId(schemaId);
    rheosEvent.setEventId(UUID.randomUUID().toString());

    for (Schema.Field field : v.getSchema().getFields()) {
      String fn = field.name();
      Object fv = v.get(fn);
      if (fv != null) {
        rheosEvent.put(fn, fv);
      }
    }
    return rheosEvent;
  }


  @Override
  public void flush() {
    producer.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String s) {
    return producer.partitionsFor(s);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return producer.metrics();
  }

  @Override
  public void close() {
    producer.close();
  }

  @Override
  public void close(long l, TimeUnit timeUnit) {
    producer.close(l, timeUnit);
  }

  // not support transaction mode for now
  @Override
  public void initTransactions() {

  }

  @Override
  public void beginTransaction() throws ProducerFencedException {

  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws
    ProducerFencedException {

  }

  @Override
  public void commitTransaction() throws ProducerFencedException {

  }

  @Override
  public void abortTransaction() throws ProducerFencedException {

  }
}
