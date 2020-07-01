package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV4;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FilterMessageSinkApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, FilterMessageV4> {

  public static void main(String[] args) throws Exception {
    FilterMessageSinkApp transformApp = new FilterMessageSinkApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.FILTER_MESSAGE_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.FILTER_MESSAGE_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Path getSinkBasePath() {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.FILTER_MESSAGE_SINK_APP_HDFS_PROPERTIES);
    return new Path(properties.getProperty(PropertyConstants.PATH));
  }

  @Override
  protected StreamingFileSink<FilterMessageV4> getStreamingFileSink() {
    return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
            .build();
  }

  @Override
  protected BulkWriter.Factory<FilterMessageV4> getSinkWriterFactory() {
    return ParquetAvroWriters.forSpecificRecord(FilterMessageV4.class);
  }

  @Override
  protected DataStream<FilterMessageV4> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.map(new TransformRichMapFunction());
  }

  private static class TransformRichMapFunction extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, FilterMessageV4> {
    private Schema rheosHeaderSchema;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
    }

    @Override
    public FilterMessageV4 map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      return FilterMessage.decodeRheos(rheosHeaderSchema, consumerRecord.value());
    }
  }

  @Override
  protected BucketAssigner<FilterMessageV4, String> getSinkBucketAssigner() {
    return new CustomEventDateTimeBucketAssigner();
  }

  private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<FilterMessageV4, String> {
    public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());

    @Override
    public String getBucketId(FilterMessageV4 element, Context context) {
      return StringConstants.BUCKET_PREFIX + EVENT_DT_FORMATTER.format(Instant.ofEpochMilli(element.getTimestamp()));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }
}
