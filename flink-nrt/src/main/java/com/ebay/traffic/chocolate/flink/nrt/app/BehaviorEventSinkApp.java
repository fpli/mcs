package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.BehaviorEvent;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.function.SherlockioMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.transformer.BehaviorEventTransformer;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Receive behavior messages, apply etl, and sink parquet files to HDFS.
 *
 * @author Zhiyuan Wang
 * @since 2020/9/14
 */
public class BehaviorEventSinkApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, BehaviorEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BehaviorEventSinkApp.class);

  private static final String INCOMING = "BehaviorEventIncoming";
  private static final String LATENCY = "BehaviorEventLatency";

  public static void main(String[] args) throws Exception {
    BehaviorEventSinkApp sinkApp = new BehaviorEventSinkApp();
    sinkApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Path getSinkBasePath() {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_HDFS_PROPERTIES);
    return new Path(properties.getProperty(PropertyConstants.PATH));
  }

  @Override
  protected StreamingFileSink<BehaviorEvent> getStreamingFileSink() {
    return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
            .build();
  }

  @Override
  protected BulkWriter.Factory<BehaviorEvent> getSinkWriterFactory() {
    return ParquetAvroWriters.forSpecificRecord(BehaviorEvent.class);
  }

  @Override
  protected DataStream<BehaviorEvent> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.map(new TransformRichMapFunction());
  }

  protected static class TransformRichMapFunction extends SherlockioMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, BehaviorEvent> {
    private transient GenericRecordDomainDataDecoder decoder;
    private transient RheosEventDeserializer deserializer;
    private SherlockioMetrics sherlockioMetrics;


    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      deserializer = new RheosEventDeserializer();
      Map<String, Object> config = new HashMap<>();
      Properties properties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
      String rheosServiceUrl = properties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      decoder = new GenericRecordDomainDataDecoder(config);
      sherlockioMetrics = SherlockioMetrics.getInstance();
      sherlockioMetrics.setJobName(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_JOBNAME);
    }

    @Override
    public BehaviorEvent map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      Long currentTimestamp = System.currentTimeMillis();
      RheosEvent rheosEvent = deserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
      GenericRecord sourceRecord = decoder.decode(rheosEvent);
      BehaviorEventTransformer behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
      BehaviorEvent behaviorEvent = new BehaviorEvent();
      behaviorEventTransformer.transform(behaviorEvent);
      sherlockioMetrics.meter(INCOMING, 1, Field.of(TransformerConstants.CHANNEL_TYPE, behaviorEvent.getChanneltype()), Field.of(TransformerConstants.CHANNEL_ACTION, behaviorEvent.getChannelaction()));
      sherlockioMetrics.meanByHistogram(LATENCY, currentTimestamp - behaviorEvent.getEventtimestamp(), Field.of(TransformerConstants.CHANNEL_TYPE, behaviorEvent.getChanneltype()), Field.of(TransformerConstants.CHANNEL_ACTION, behaviorEvent.getChannelaction()));
      return behaviorEvent;
    }
  }

  @Override
  protected BucketAssigner<BehaviorEvent, String> getSinkBucketAssigner() {
    return new CustomEventDateTimeBucketAssigner();
  }

  /**
   * Assigns to buckets based on event timestamp.
   *
   * <p>The {@code CustomEventDateTimeBucketAssigner} will create directories of the following form:
   * {@code /{basePath}/{dateTimePath}/}. The {@code basePath} is the path
   * that was specified as a base path when creating the
   * {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink}.
   * The {@code dateTimePath} is determined based on the event timestamp.
   *
   *
   * <p>This will create for example the following bucket path:
   * {@code /base/dt=1976-12-31/}
   */
  protected static class CustomEventDateTimeBucketAssigner implements BucketAssigner<BehaviorEvent, String> {
    public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());

    @Override
    public String getBucketId(BehaviorEvent element, Context context) {
      return StringConstants.DATE_BUCKET_PREFIX + EVENT_DT_FORMATTER.format(Instant.ofEpochMilli(element.getEventtimestamp()));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }
}
