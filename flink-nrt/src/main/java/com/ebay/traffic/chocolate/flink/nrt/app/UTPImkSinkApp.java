package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingImkMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.function.FilterDuplicatedEventsByRvrId;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.parquet.CompressionParquetAvroWriters;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Receive filter messages from Kafka topics, and sink parquet files to HDFS directly.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class UTPImkSinkApp {
  private static String cluster;

  public static final String CHECKPOINT_DATA_URI = "file:///data/checkpoint/utp-event-imk-sink";

  protected StreamExecutionEnvironment streamExecutionEnvironment;

  // Time interval between state checkpoints in milliseconds
  protected static final long CHECK_POINT_PERIOD = TimeUnit.SECONDS.toMillis(180);

  // The minimal pause before the next checkpoint is triggered
  protected static final long MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(1);

  // The checkpoint timeout, in milliseconds
  protected static final long CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(300);

  // The maximum number of concurrent checkpoint attempts
  protected static final int MAX_CONCURRENT_CHECK_POINTS = 1;

  private static final OutputTag<UnifiedTrackingImkMessage> DUP_TAG = new OutputTag<UnifiedTrackingImkMessage>("dup"){};

  public static final String RNO_PATH = "rno.path";
  public static final String RNO_DUP_PATH = "rno.dup.path";
  public static final String HERCULES_PATH = "hercules.path";
  public static final String HERCULES_DUP_PATH = "hercules.dup.path";

  public static void main(String[] args) throws Exception {
    UTPImkSinkApp transformApp = new UTPImkSinkApp();
    transformApp.run(args);
  }

  protected void run(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    String enableDedupe = parameter.get("enableDedupe", Boolean.TRUE.toString());
    cluster = parameter.get("cluster");

    Properties sinkProperties = PropertyMgr.getInstance().loadProperty(PropertyConstants.UTP_IMK_SINK_APP_HDFS_PROPERTIES);
    Path path;
    Path dupPath;
    if ("rno".equalsIgnoreCase(cluster)) {
      path = new Path(sinkProperties.getProperty(RNO_PATH));
      dupPath = new Path(sinkProperties.getProperty(RNO_DUP_PATH));
    } else if ("hercules".equalsIgnoreCase(cluster)) {
      path = new Path(sinkProperties.getProperty(HERCULES_PATH));
      dupPath = new Path(sinkProperties.getProperty(HERCULES_DUP_PATH));
    } else {
      throw new IllegalArgumentException("Please set cluster rno or hercules firstly!");
    }

    String jobName = String.format("%s-%s", this.getClass().getSimpleName(), cluster);

    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    streamExecutionEnvironment.enableCheckpointing(CHECK_POINT_PERIOD);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    RocksDBStateBackend stateBackend = new RocksDBStateBackend(String.format("%s/%s", CHECKPOINT_DATA_URI, cluster), true);
    stateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
    streamExecutionEnvironment.setStateBackend(stateBackend);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    SingleOutputStreamOperator<UnifiedTrackingImkMessage> mainStream = streamExecutionEnvironment
            .addSource(getKafkaConsumer()).name("source").uid("source")
            .map(new TransformRichMapFunction()).name("deserialize").uid("deserialize")
            .keyBy(UnifiedTrackingImkMessage::getRvrId)
            .process(new FilterDuplicatedEventsByRvrId(jobName, Boolean.parseBoolean(enableDedupe), DUP_TAG)).name("dedupe").uid("dedupe");

    mainStream.addSink(getSink(path));

    mainStream.getSideOutput(DUP_TAG).addSink(getSink(dupPath)).name("sink-dup").uid("sink-dup");
    streamExecutionEnvironment.execute(jobName);
  }

  private StreamingFileSink<UnifiedTrackingImkMessage> getSink(Path path) {
    return StreamingFileSink.forBulkFormat(path, getSinkWriterFactory())
            .withBucketAssigner(getSinkBucketAssigner())
            .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("utp-imk").withPartSuffix(".snappy.parquet").build())
            .build();
  }

  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.UTP_IMK_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  protected Properties getConsumerProperties() {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.UTP_IMK_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
    if ("hercules".equalsIgnoreCase(cluster)) {
      String property = properties.getProperty("group.id");
      properties.setProperty("group.id", String.format("%s-%s", property, cluster));
    }
    return properties;
  }

  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  protected BulkWriter.Factory<UnifiedTrackingImkMessage> getSinkWriterFactory() {
    return CompressionParquetAvroWriters.forSpecificRecord(UnifiedTrackingImkMessage.class, CompressionCodecName.SNAPPY);
  }

  protected static class TransformRichMapFunction extends RichMapFunction<ConsumerRecord<byte[], byte[]>, UnifiedTrackingImkMessage> {
    private transient DatumReader<UnifiedTrackingImkMessage> imkReader;

    @Override
    public void open(Configuration parameters) throws Exception {
      imkReader = new SpecificDatumReader<>(UnifiedTrackingImkMessage.getClassSchema());
    }

    @Override
    public UnifiedTrackingImkMessage map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(consumerRecord.value(), null);
      UnifiedTrackingImkMessage datum = new UnifiedTrackingImkMessage();
      datum = imkReader.read(datum, decoder);
      return datum;
    }
  }

  protected BucketAssigner<UnifiedTrackingImkMessage, String> getSinkBucketAssigner() {
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
  private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<UnifiedTrackingImkMessage, String> {
    @Override
    public String getBucketId(UnifiedTrackingImkMessage element, Context context) {
      return StringConstants.DATE_BUCKET_PREFIX + element.getEventDt();
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }
}
