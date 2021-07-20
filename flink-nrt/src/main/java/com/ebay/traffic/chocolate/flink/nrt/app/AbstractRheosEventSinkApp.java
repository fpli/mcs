package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.GenericRecordKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.parquet.CompressionParquetAvroWriters;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Receive messages from rheos, and sink parquet files to HDFS directly as the topic schema.
 *
 * @author Zhiyuan Wang
 * @since 2020/11/18
 */
@SuppressWarnings("unchecked")
public abstract class AbstractRheosEventSinkApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRheosEventSinkApp.class);
  protected Schema schema;
  protected String rheosServicesUrls;
  protected String schemaName;

  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.MINUTES.toMillis(3);

  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(30);

  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.MINUTES.toMillis(3);

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  protected Map<String, Object> config;

  protected void run() throws Exception {
    loadProperty();
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    addSink(addSource());
    execute();
  }

  private void execute() throws Exception {
    Map<String, Object> job = (Map<String, Object>) config.get("job");
    streamExecutionEnvironment.execute((String) job.get("name"));
  }

  protected SingleOutputStreamOperator<GenericRecord> addSource() throws IOException {
    Map<String, Object> source = (Map<String, Object>) config.get("source");
    String name = (String) source.get("name");
    String uid = (String) source.get("uid");
    return streamExecutionEnvironment
            .addSource(buildConsumer()).returns(new GenericRecordAvroTypeInfo(schema)).name(name).uid(uid);
  }

  protected void addSink(SingleOutputStreamOperator<GenericRecord> output) {
    Map<String, Object> sink = (Map<String, Object>) config.get("sink");

    for (Map.Entry<String, Object> entry : sink.entrySet()) {
      Map<String, Object> value = (Map<String, Object>) entry.getValue();
      String path = (String) value.get("path");
      String name = (String) value.get("name");
      String uid = (String) value.get("uid");

      Map<String, Object> outputFileConfig = (Map<String, Object>) value.get("outputFileConfig");
      String partPrefix = (String) outputFileConfig.get("partPrefix");
      String partSuffix = (String) outputFileConfig.get("partSuffix");

      output.addSink(StreamingFileSink.forBulkFormat(new Path(path), getSinkWriterFactory())
              .withBucketAssigner(getSinkBucketAssigner())
              .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix(partPrefix).withPartSuffix(partSuffix).build())
              .build()).name(name).uid(uid);
    }
  }

  protected FlinkKafkaConsumer<GenericRecord> buildConsumer() throws IOException {
    Map<String, Object> source = (Map<String, Object>) config.get("source");
    String topics = (String) source.get("topic");

    Properties consumerProperties = new Properties();
    consumerProperties.load(new StringReader((String) source.get("properties")));

    schemaName = consumerProperties.getProperty(RheosConstants.RHEOS_TOPIC_SCHEMA);
    rheosServicesUrls = consumerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
    schema = getSchema();

    return new FlinkKafkaConsumer<>(Arrays.asList(topics.split(",")), new GenericRecordKafkaDeserializationSchema(schema, rheosServicesUrls), consumerProperties);
  }

  protected void prepareBaseExecutionEnvironment() {
    streamExecutionEnvironment.enableCheckpointing(DEFAULT_CHECK_POINT_PERIOD);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(DEFAULT_CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(DEFAULT_MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

  /**
   * Sink events to compressed parquest files by default.
   * @return writer factory
   */
  protected BulkWriter.Factory<GenericRecord> getSinkWriterFactory() {
    return CompressionParquetAvroWriters.forGenericRecord(schema, CompressionCodecName.SNAPPY);
  }

  /**
   * Get the latest schema by name by default.
   * @return schema
   */
  protected Schema getSchema() {
    Map<String, Object> map = new HashMap<>();
    map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServicesUrls);
    SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
            new SchemaRegistryAwareAvroSerializerHelper<>(map, GenericRecord.class);
    return serializerHelper.getSchema(schemaName);
  }

  protected abstract void loadProperty();
  protected abstract BucketAssigner<GenericRecord, String> getSinkBucketAssigner();

}
