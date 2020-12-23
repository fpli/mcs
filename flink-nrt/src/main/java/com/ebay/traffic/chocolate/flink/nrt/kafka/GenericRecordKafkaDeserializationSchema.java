package com.ebay.traffic.chocolate.flink.nrt.kafka;

import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Decode rheos message as generic record.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class GenericRecordKafkaDeserializationSchema implements KafkaDeserializationSchema<GenericRecord> {
  private transient GenericRecordDomainDataDecoder decoder;
  private transient RheosEventDeserializer deserializer;
  private final transient Schema schema;
  private final String rheosServiceUrl;

  public GenericRecordKafkaDeserializationSchema(Schema schema, String rheosServiceUrl) {
    this.schema = schema;
    this.rheosServiceUrl = rheosServiceUrl;
  }

  @Override
  public GenericRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    if (this.deserializer == null) {
      this.deserializer = new RheosEventDeserializer();
    }
    if (this.decoder == null) {
      Map<String, Object> config = new HashMap<>();
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      this.decoder = new GenericRecordDomainDataDecoder(config);
    }
    RheosEvent sourceRheosEvent = this.deserializer.deserialize(record.topic(), record.value());
    return this.decoder.decode(sourceRheosEvent);
  }

  @Override
  public TypeInformation<GenericRecord> getProducedType() {
    return new GenericRecordAvroTypeInfo(schema);
  }

  @Override
  public boolean isEndOfStream(GenericRecord nextElement) {
    return false;
  }
}
