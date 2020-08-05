package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.UBIEvent;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.monitoring.ESMetrics;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class UBIEventSinkApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, UBIEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(UBIEventSinkApp.class);

  public static void main(String[] args) throws Exception {
    UBIEventSinkApp transformApp = new UBIEventSinkApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.UBI_EVENT_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.UBI_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Path getSinkBasePath() {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.UBI_EVENT_SINK_APP_HDFS_PROPERTIES);
    return new Path(properties.getProperty(PropertyConstants.PATH));
  }

  @Override
  protected StreamingFileSink<UBIEvent> getStreamingFileSink() {
    return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
            .build();
  }

  @Override
  protected BulkWriter.Factory<UBIEvent> getSinkWriterFactory() {
    return ParquetAvroWriters.forSpecificRecord(UBIEvent.class);
  }

  @Override
  protected DataStream<UBIEvent> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.map(new TransformRichMapFunction());
  }

  protected static class TransformRichMapFunction extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, UBIEvent> {
    public static final String APPLICATION_PAYLOAD = "applicationPayload";
    public static final String CLIENT_DATA = "clientData";
    private transient GenericRecordDomainDataDecoder decoder;
    private transient RheosEventDeserializer deserializer;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      deserializer = new RheosEventDeserializer();
      Map<String, Object> config = new HashMap<>();
      Properties properties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UBI_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
      String rheosServiceUrl = properties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      decoder = new GenericRecordDomainDataDecoder(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public UBIEvent map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      RheosEvent rheosEvent = deserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
      GenericRecord sourceRecord = decoder.decode(rheosEvent);
      UBIEvent ubiEvent = new UBIEvent();
      for (Schema.Field field : UBIEvent.getClassSchema().getFields()) {
        String name = field.name();
        Object value = sourceRecord.get(name);
        if (name.equals(APPLICATION_PAYLOAD) || name.equals(CLIENT_DATA)) {
          ubiEvent.put(name, convertMap((HashMap<Utf8, Utf8>) value));
          continue;
        }
        if (value instanceof Utf8) {
          ubiEvent.put(name, String.valueOf(value));
          continue;
        }
        ubiEvent.put(name, value);
      }
      return ubiEvent;
    }

    private String convertMap(HashMap<Utf8, Utf8> value) {
      StringJoiner joiner = new StringJoiner(StringConstants.AND);
      value.forEach((k, v) -> joiner.add(k + StringConstants.EQUAL + v));
      return joiner.toString();
    }
  }

  @Override
  protected BucketAssigner<UBIEvent, String> getSinkBucketAssigner() {
    return new CustomEventDateTimeBucketAssigner();
  }

  private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<UBIEvent, String> {
    public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());

    @Override
    public String getBucketId(UBIEvent element, Context context) {
      return StringConstants.BUCKET_PREFIX + EVENT_DT_FORMATTER.format(Instant.ofEpochMilli(element.getEventTimestamp()));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }
}
