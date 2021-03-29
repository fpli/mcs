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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Receive messages from rheos, and sink parquet files to HDFS directly as the topic schema.
 *
 * @author Zhiyuan Wang
 * @since 2020/11/18
 */
public abstract class AbstractRheosEventSinkApp extends AbstractRheosHDFSCompatibleApp<GenericRecord, GenericRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRheosEventSinkApp.class);
  protected Schema schema;
  protected String rheosServicesUrls;
  protected String schemaName;

  @Override
  protected void run() throws Exception {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    Properties consumerProperties = getConsumerProperties();
    schemaName = consumerProperties.getProperty(RheosConstants.RHEOS_TOPIC_SCHEMA);
    rheosServicesUrls = consumerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
    schema = getSchema();
    SingleOutputStreamOperator<GenericRecord> output = streamExecutionEnvironment.addSource(getKafkaConsumer())
            .returns(new GenericRecordAvroTypeInfo(schema));
    output.addSink(getStreamingFileSink());
    streamExecutionEnvironment.execute(this.getClass().getSimpleName());
  }

  /**
   * Get consumer topics from config file
   * @return kafka consumer topics
   */
  protected abstract List<String> getConsumerTopics();

  /**
   * Get properties from config file
   * @return kafka consumer properties
   */
  protected abstract Properties getConsumerProperties();

  @Override
  protected FlinkKafkaConsumer<GenericRecord> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new GenericRecordKafkaDeserializationSchema(schema, rheosServicesUrls), getConsumerProperties());
  }

  protected abstract Path getSinkBasePath();

  /**
   * Sink events to compressed parquest files by default.
   * @return writer factory
   */
  @Override
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

  protected abstract BucketAssigner<GenericRecord, String> getSinkBucketAssigner();

  @Override
  protected DataStream<GenericRecord> transform(DataStreamSource<GenericRecord> dataStreamSource) {
    return null;
  }
}
