package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.ImkRvrTrckngEventMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
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

public class ImkRvrTrckngEventSinkApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, ImkRvrTrckngEventMessage> {

  public static void main(String[] args) throws Exception {
    ImkRvrTrckngEventSinkApp transformApp = new ImkRvrTrckngEventSinkApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Path getSinkBasePath() {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_SINK_APP_HDFS_PROPERTIES);
    return new Path(properties.getProperty(PropertyConstants.PATH));
  }

  @Override
  protected StreamingFileSink<ImkRvrTrckngEventMessage> getStreamingFileSink() {
    return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
            .build();
  }

  @Override
  protected BulkWriter.Factory<ImkRvrTrckngEventMessage> getSinkWriterFactory() {
    return ParquetAvroWriters.forSpecificRecord(ImkRvrTrckngEventMessage.class);
  }

  @Override
  protected DataStream<ImkRvrTrckngEventMessage> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.map(new TransformRichMapFunction());
  }

  private static class TransformRichMapFunction extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, ImkRvrTrckngEventMessage> {
    private transient DatumReader<ImkRvrTrckngEventMessage> imkReader;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      imkReader = new SpecificDatumReader<>(ImkRvrTrckngEventMessage.getClassSchema());
    }

    @Override
    public ImkRvrTrckngEventMessage map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(consumerRecord.value(), null);
      ImkRvrTrckngEventMessage datum = new ImkRvrTrckngEventMessage();
      datum = imkReader.read(datum, decoder);
      return datum;
    }
  }

  @Override
  protected BucketAssigner<ImkRvrTrckngEventMessage, String> getSinkBucketAssigner() {
    return new CustomEventDateTimeBucketAssigner();
  }

  private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<ImkRvrTrckngEventMessage, String> {

    @Override
    public String getBucketId(ImkRvrTrckngEventMessage element, Context context) {
      return StringConstants.BUCKET_PREFIX + element.getEventDt();
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }
}
