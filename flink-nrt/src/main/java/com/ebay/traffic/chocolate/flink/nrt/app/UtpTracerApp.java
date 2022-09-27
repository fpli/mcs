/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * Consume UTP topic and write ES for real-time event tracer
 *
 * @author Xiang Li
 * @since 2020/11/17
 */
public class UtpTracerApp
    extends AbstractRheosElasticSearchCompatibleApp<ConsumerRecord<byte[], byte[]>, UnifiedTrackingRheosMessage>  {
  private static final Logger LOGGER = LoggerFactory.getLogger(UtpTracerApp.class);
  // default index prefix
  static final String INDEX_PREFIX = "utp-tracer-write";
  static final String DEFAULT_TYPE = "_doc";
  static final String PAYLOAD = "payload";
  static final List<String> HIDE_FIELDS = new ArrayList<>();
  static {
    HIDE_FIELDS.add("to-email");
    HIDE_FIELDS.add("sndr_mail");
    HIDE_FIELDS.add("user");
    HIDE_FIELDS.add("timestamp");
    HIDE_FIELDS.add("annotation.mesg.list.test");
    HIDE_FIELDS.add("ch.com99");
  }

  public static void main(String[] args) throws Exception {
    UtpTracerApp rheosESCompatibleApp = new UtpTracerApp();
    rheosESCompatibleApp.run();
  }

  private static class Deserialize extends RichMapFunction<ConsumerRecord<byte[], byte[]>, UnifiedTrackingRheosMessage> {
    private transient DatumReader<GenericRecord> rheosHeaderReader;
    private transient DatumReader<UnifiedTrackingRheosMessage> reader;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      Schema rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
      rheosHeaderReader = new GenericDatumReader<>(rheosHeaderSchema);
      reader = new SpecificDatumReader<>(UnifiedTrackingRheosMessage.getClassSchema());
    }

    @Override
    public UnifiedTrackingRheosMessage map(ConsumerRecord<byte[], byte[]> value) throws Exception {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value.value(), null);
      rheosHeaderReader.read(null, decoder);
      UnifiedTrackingRheosMessage datum = new UnifiedTrackingRheosMessage();
      datum = reader.read(datum, decoder);
      return datum;
    }
  }

  @Override
  protected DataStream<UnifiedTrackingRheosMessage> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.map(new Deserialize()).map(new UtpMonitor());
  }

  @Override
  protected List<String> getConsumerTopics() {
    return Arrays.asList(PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.UTP_EVENT_TRACER_CONSUMER_TOPIC_PROPERTIES)
        .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.UTP_EVENT_TRACER_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(),
        new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Properties getElasticSearchProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.UTP_EVENT_TRACER_ES_PROPERTIES);
  }

  @Override
  protected ElasticsearchSinkFunction<UnifiedTrackingRheosMessage> getElasticSearchSinkFunction() {
    return new CustomElasticsearchSinkFunction();
  }

  private static class CustomElasticsearchSinkFunction implements ElasticsearchSinkFunction<UnifiedTrackingRheosMessage> {
    public IndexRequest createIndexRequest(UnifiedTrackingRheosMessage message) {
      String element = message.toString();
      final String id = UUID.randomUUID().toString();

      Type type = new TypeToken<Map<String, Object>>() {}.getType();
      Gson gson = new Gson();
      Map<String, Object> json = gson.fromJson(element, type);
      // use utp event ts as the es timestamp
      String eventTsString = json.get("eventTs").toString();
      Long eventTs = Longs.tryParse(eventTsString);
      ZonedDateTime timestamp;
      if(eventTs == null) {
        timestamp = ZonedDateTime.now();
      } else {
        Instant instant = Instant.ofEpochMilli(eventTs);
        timestamp = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
      }
      json.put("@timestamp", timestamp);
      // remove fields should not be shown due to PII or other legal requirements
      Map<String, Object> payload = (Map<String, Object>)json.get(PAYLOAD);
      for (String key :
          HIDE_FIELDS) {
        payload.remove(key);
      }
      return Requests.indexRequest()
          .index(INDEX_PREFIX)
          .type(DEFAULT_TYPE)
          .id(id)
          .source(json);
    }

    @Override
    public void process(UnifiedTrackingRheosMessage message, RuntimeContext ctx, RequestIndexer indexer) {
      indexer.add(createIndexRequest(message));
    }
  }
}
