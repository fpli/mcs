package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.parquet.CompressionParquetAvroWriters;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Receive utp messages, and sink parquet files to HDFS directly.
 *
 * @author Zhiyuan Wang
 * @since 2020/11/18
 */
public class UTPEventSinkApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, UnifiedTrackingRheosMessage> {
  private static final Logger LOGGER = LoggerFactory.getLogger(UTPEventSinkApp.class);

  public static void main(String[] args) throws Exception {
    UTPEventSinkApp sinkApp = new UTPEventSinkApp();
    sinkApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.UTP_EVENT_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.UTP_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Path getSinkBasePath() {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.UTP_EVENT_SINK_APP_HDFS_PROPERTIES);
    return new Path(properties.getProperty(PropertyConstants.PATH));
  }

  @Override
  protected StreamingFileSink<UnifiedTrackingRheosMessage> getStreamingFileSink() {
    return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
            .build();
  }

  @Override
  protected BulkWriter.Factory<UnifiedTrackingRheosMessage> getSinkWriterFactory() {
    return ParquetAvroWriters.forSpecificRecord(UnifiedTrackingRheosMessage.class);
  }

  @Override
  protected DataStream<UnifiedTrackingRheosMessage> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.map(new TransformRichMapFunction());
  }

  protected static class TransformRichMapFunction extends RichMapFunction<ConsumerRecord<byte[], byte[]>, UnifiedTrackingRheosMessage> {
    private transient DatumReader<UnifiedTrackingRheosMessage> reader;
    private transient DatumReader<GenericRecord> rheosHeaderReader;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      reader = new SpecificDatumReader<>(UnifiedTrackingRheosMessage.getClassSchema());
      rheosHeaderReader = new GenericDatumReader<>(RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema());
    }

    @Override
    public UnifiedTrackingRheosMessage map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(consumerRecord.value(), null);
      UnifiedTrackingRheosMessage datum = new UnifiedTrackingRheosMessage();
      // skips the rheos header
      rheosHeaderReader.read(null, decoder);
      return reader.read(datum, decoder);
    }
  }

  @Override
  protected BucketAssigner<UnifiedTrackingRheosMessage, String> getSinkBucketAssigner() {
    return new CustomEventDateTimeBucketAssigner();
  }

  /**
   * Assigns to buckets based on event timestamp.
   *
   * <p>The {@code CustomEventDateTimeBucketAssigner} will create directories of the following form:
   * {@code /{basePath}/{dateTimePath}/{hourPath}. The {@code basePath} is the path
   * that was specified as a base path when creating the
   * {@link StreamingFileSink}.
   * The {@code dateTimePath} and {hourPath} is determined based on the event timestamp.
   *
   *
   * <p>This will create for example the following bucket path:
   * {@code /base/dt=1976-12-31/hour=01}
   */
  private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<UnifiedTrackingRheosMessage, String> {
    public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());
    public static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.HH).withZone(ZoneId.systemDefault());

    @Override
    public String getBucketId(UnifiedTrackingRheosMessage element, Context context) {
      Long eventTs = element.getEventTs();
      Instant instant = Instant.ofEpochMilli(eventTs);
      String eventDt = EVENT_DT_FORMATTER.format(instant);
      String hour = HOUR_FORMATTER.format(instant);
      return StringConstants.DATE_BUCKET_PREFIX + eventDt + StringConstants.SLASH + StringConstants.HOUR_BUCKET_PREFIX + hour;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }
}
