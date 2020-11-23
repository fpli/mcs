package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.ImkTrckngEventWideMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Receive imk tracking event messages and sink them to HDFS directly.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class ImkTrckngEventSinkApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, ImkTrckngEventWideMessage> {

  public static void main(String[] args) throws Exception {
    ImkTrckngEventSinkApp transformApp = new ImkTrckngEventSinkApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.IMK_TRCKNG_EVENT_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_TRCKNG_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Path getSinkBasePath() {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_TRCKNG_EVENT_SINK_APP_HDFS_PROPERTIES);
    return new Path(properties.getProperty(PropertyConstants.PATH));
  }

  @Override
  protected StreamingFileSink<ImkTrckngEventWideMessage> getStreamingFileSink() {
    return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
            .build();
  }

  @Override
  protected BulkWriter.Factory<ImkTrckngEventWideMessage> getSinkWriterFactory() {
    return ParquetAvroWriters.forSpecificRecord(ImkTrckngEventWideMessage.class);
  }

  @Override
  protected DataStream<ImkTrckngEventWideMessage> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.map(new TransformRichMapFunction());
  }

  protected static class TransformRichMapFunction extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, ImkTrckngEventWideMessage> {
    private transient DatumReader<ImkTrckngEventWideMessage> imkReader;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      imkReader = new SpecificDatumReader<>(ImkTrckngEventWideMessage.getClassSchema());
    }

    @Override
    public ImkTrckngEventWideMessage map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(consumerRecord.value(), null);
      ImkTrckngEventWideMessage datum = new ImkTrckngEventWideMessage();
      datum = imkReader.read(datum, decoder);
      return datum;
    }
  }

  @Override
  protected BucketAssigner<ImkTrckngEventWideMessage, String> getSinkBucketAssigner() {
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
  private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<ImkTrckngEventWideMessage, String> {

    @Override
    public String getBucketId(ImkTrckngEventWideMessage element, Context context) {
      return StringConstants.BUCKET_PREFIX + element.getEventDt();
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }
}
