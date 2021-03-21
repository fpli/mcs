package com.ebay.traffic.chocolate.flink.nrt.app;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.SlidingWindowReservoir;
import com.ebay.app.raptor.chocolate.avro.RheosHeader;
import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingImkMessage;
import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.function.FilterDuplicatedEventsByRvrId;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.provider.ersxid.ErsXidRequest;
import com.ebay.traffic.chocolate.flink.nrt.transformer.utp.UTPImkTransformer;
import com.ebay.traffic.chocolate.flink.nrt.transformer.utp.UTPImkTransformerMetrics;
import com.ebay.traffic.chocolate.flink.nrt.transformer.utp.UTPRoiTransformer;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.google.common.primitives.Longs;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Receive performance marketing events from utp topic, transform them to imk schema and send them to imk topic.
 * If need to disable dedupe by rvr_id, please add command line parameter --enableDedupe false
 *
 * @author Zhiyuan Wang
 * @since 2020/12/28
 */
public class UTPImkTransformApp {
  public static final String CHECKPOINT_DATA_URI = "file:///data/checkpoint/utp-event-imk-transform";
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

  public static void main(String[] args) throws Exception {
    UTPImkTransformApp transformApp = new UTPImkTransformApp();
    transformApp.run(args);
  }

  protected void run(String[] args) throws Exception {
    String jobName = this.getClass().getSimpleName();
    ParameterTool parameter = ParameterTool.fromArgs(args);
    String enableDedupe = parameter.get("enableDedupe", Boolean.TRUE.toString());
    Properties topicProperties = PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UTP_IMK_TRANSFORM_APP_RHEOS_PRODUCER_TOPIC_PROPERTIES);
    String topic = topicProperties.getProperty(PropertyConstants.TOPIC);
    String dupTopic = topicProperties.getProperty("dup.topic");

    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment(args);
    SingleOutputStreamOperator<UnifiedTrackingImkMessage> utpImkStream = streamExecutionEnvironment.addSource(getKafkaConsumer()).name("source").uid("source")
            .map(new Deserialize()).name("deserialize").uid("deserialize")
            .filter(new Filter()).name("filter").uid("filter")
            .map(new Transform()).name("transform").uid("transform");
    SingleOutputStreamOperator<UnifiedTrackingImkMessage> mainStream = utpImkStream
            .keyBy(new GetRvrId())
            .process(new FilterDuplicatedEventsByRvrId(jobName, Boolean.parseBoolean(enableDedupe), DUP_TAG)).name("dedupe").uid("dedupe");
    AsyncDataStream.unorderedWait(mainStream, new ErsXidRequest(), 10000, TimeUnit.MILLISECONDS, 1000)
            .name("ersxid").uid("ersxid")
            .map(new SerializeRheosEvent(topic)).name("serialize").uid("serialize")
            .addSink(getKafkaProducer()).name("sink").name("sink");
    mainStream.getSideOutput(DUP_TAG)
            .map(new SerializeRheosEvent(dupTopic)).name("serialize-dup").uid("serialize-dup")
            .addSink(getDupKafkaProducer()).name("sink-dup").uid("sink-dup");
    streamExecutionEnvironment.execute(jobName);
  }

  @SuppressWarnings("UnstableApiUsage")
  protected void prepareBaseExecutionEnvironment(String[] args) throws IOException {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    String latencyTrackingInterval = parameter.get("latencyTrackingInterval");
    if (StringUtils.isNumeric(latencyTrackingInterval)) {
      Long aLong = Longs.tryParse(latencyTrackingInterval);
      if (aLong != null) {
        ExecutionConfig config = streamExecutionEnvironment.getConfig();
        config.setLatencyTrackingInterval(TimeUnit.SECONDS.toMillis(aLong));
      }
    }
    streamExecutionEnvironment.enableCheckpointing(CHECK_POINT_PERIOD);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    RocksDBStateBackend stateBackend = new RocksDBStateBackend(CHECKPOINT_DATA_URI, true);
    stateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
    streamExecutionEnvironment.setStateBackend(stateBackend);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

  protected List<String> getConsumerTopics() {
    return Arrays.asList(PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UTP_IMK_TRANSFORM_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
            .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UTP_IMK_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(),
            new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  protected FlinkKafkaProducer<Tuple3<String, Long, byte[]>> getKafkaProducer() {
    return new FlinkKafkaProducer<>(StringConstants.EMPTY, new DefaultKafkaSerializationSchema(),
            getProducerProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
  }

  protected FlinkKafkaProducer<Tuple3<String, Long, byte[]>> getDupKafkaProducer() {
    return new FlinkKafkaProducer<>(StringConstants.EMPTY, new DefaultKafkaSerializationSchema(),
            getProducerProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
  }

  protected Properties getProducerProperties() {
    return PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UTP_IMK_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
  }

  protected static class SerializeRheosEvent extends RichMapFunction<UnifiedTrackingImkMessage, Tuple3<String, Long, byte[]>> {
    private transient EncoderFactory encoderFactory;
    private final String topic;

    public SerializeRheosEvent(String topic) {
      this.topic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      encoderFactory = EncoderFactory.get();
    }

    @Override
    public Tuple3<String, Long, byte[]> map(UnifiedTrackingImkMessage value) throws Exception {
      return new Tuple3<>(this.topic, value.getRvrId(), serializeRheosEvent(getRheosEvent(value)));
    }

    public RheosEvent getRheosEvent(GenericRecord v) {
      return new RheosEvent(v);
    }

    private byte[] serializeRheosEvent(RheosEvent data) {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = getWriter(data);
        writer.write(data, encoder);
        encoder.flush();
        return out.toByteArray();
      } catch (Exception e) {
        throw new SerializationException(e);
      }
    }

    private DatumWriter<GenericRecord> getWriter(RheosEvent rheosEvent) {
      return new GenericDatumWriter<>(rheosEvent.getSchema());
    }

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
      // skips the rheos header
      rheosHeaderReader.read(null, decoder);
      UnifiedTrackingRheosMessage datum = new UnifiedTrackingRheosMessage();
      datum = reader.read(datum, decoder);
      return datum;
    }
  }

  private static class Filter extends RichFilterFunction<UnifiedTrackingRheosMessage> {
    private transient Meter numBotRecordsInRate;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numBotRecordsInRate = getRuntimeContext().getMetricGroup().meter("numBotRecordsInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public boolean filter(UnifiedTrackingRheosMessage sourceRecord) throws Exception {
      String channelType = sourceRecord.getChannelType();
      if (StringUtils.isEmpty(channelType)) {
        return false;
      }
      String actionType = sourceRecord.getActionType();
      if (StringUtils.isEmpty(actionType)) {
        return false;
      }
      if (!isImkEvent(channelType, actionType)) {
        return false;
      }
      boolean bot = isBot(channelType, sourceRecord);
      if (bot) {
        numBotRecordsInRate.markEvent();
      }

      return !bot;
    }

    private boolean isImkEvent(String channelType, String actionType) {
      if (actionType.equals(ActionTypeEnum.ROI.getValue())) {
        return true;
      }
      if (ChannelTypeEnum.PLA.getValue().equals(channelType)) {
        return true;
      }
      if (ChannelTypeEnum.TEXT.getValue().equals(channelType)) {
        return true;
      }
      if (ChannelTypeEnum.DISPLAY.getValue().equals(channelType)) {
        return true;
      }
      if (ChannelTypeEnum.SOCIAL.getValue().equals(channelType)) {
        return true;
      }
      return ChannelTypeEnum.SEARCH_ENGINE_FREE_LISTINGS.getValue().equals(channelType);
    }

    private boolean isBot(String channelType, UnifiedTrackingRheosMessage sourceRecord) {
      if (ChannelTypeEnum.SEARCH_ENGINE_FREE_LISTINGS.getValue().equals(channelType)) {
        return sourceRecord.getIsBot();
      }
      return false;
    }
  }

  private static class Transform extends RichMapFunction<UnifiedTrackingRheosMessage, UnifiedTrackingImkMessage> {
    private int schemaId;
    private String producer;
    private transient Histogram latency;
    private transient Map<Tuple2<String, String>, Histogram> latencyByChannel;
    private transient Map<Tuple2<String, String>, Meter> numUTPImkRecordsInRateByChannel;
    private transient Map<String, Meter> etlMeters;

    @Override
    public void open(Configuration parameters) throws Exception {
      Map<String, Object> config = new HashMap<>();
      Properties producerProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UTP_IMK_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
      String rheosServiceUrl = producerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
              new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
      String schemaName = (String) producerProperties.get(RheosConstants.RHEOS_TOPIC_SCHEMA);
      schemaId = serializerHelper.getSchemaId(schemaName);
      producer = (String) producerProperties.get(RheosConstants.RHEOS_PRODUCER);

      latency = getRuntimeContext().getMetricGroup().histogram("utpImkLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));

      latencyByChannel = new HashMap<>();
      latencyByChannel.put(
              new Tuple2<>(ChannelTypeEnum.PLA.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().histogram("utpPlaClickLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)))));
      latencyByChannel.put(
              new Tuple2<>(ChannelTypeEnum.TEXT.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().histogram("utpTextClickLatency",
                      new DropwizardHistogramWrapper(
                              new com.codahale.metrics.Histogram(
                                      new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)))));
      latencyByChannel.put(
              new Tuple2<>(ChannelTypeEnum.DISPLAY.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().histogram("utpDisplayClickLatency",
                      new DropwizardHistogramWrapper(
                              new com.codahale.metrics.Histogram(
                                      new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)))));
      latencyByChannel.put(
              new Tuple2<>(ChannelTypeEnum.DISPLAY.getValue(), ActionTypeEnum.SERVE.getValue()),
              getRuntimeContext().getMetricGroup().histogram("utpDisplayServeLatency",
                      new DropwizardHistogramWrapper(
                              new com.codahale.metrics.Histogram(
                                      new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)))));
      latencyByChannel.put(
              new Tuple2<>(ChannelTypeEnum.SOCIAL.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().histogram("utpSocialClickLatency",
                      new DropwizardHistogramWrapper(
                              new com.codahale.metrics.Histogram(
                                      new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)))));
      latencyByChannel.put(
              new Tuple2<>(ChannelTypeEnum.SEARCH_ENGINE_FREE_LISTINGS.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().histogram("utpFreeListingsClickLatency",
                      new DropwizardHistogramWrapper(
                              new com.codahale.metrics.Histogram(
                                      new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)))));
      latencyByChannel.put(
              new Tuple2<>(ChannelTypeEnum.GENERIC.getValue(), ActionTypeEnum.ROI.getValue()),
              getRuntimeContext().getMetricGroup().histogram("utpRoiLatency",
                      new DropwizardHistogramWrapper(
                              new com.codahale.metrics.Histogram(
                                      new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)))));

      numUTPImkRecordsInRateByChannel = new HashMap<>();
      numUTPImkRecordsInRateByChannel.put(
              new Tuple2<>(ChannelTypeEnum.PLA.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().meter("numUTPPlaClickRecordsInRate",
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      numUTPImkRecordsInRateByChannel.put(
              new Tuple2<>(ChannelTypeEnum.TEXT.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().meter("numUTPTextClickRecordsInRate",
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      numUTPImkRecordsInRateByChannel.put(
              new Tuple2<>(ChannelTypeEnum.DISPLAY.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().meter("numUTPDisplayClickRecordsInRate",
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      numUTPImkRecordsInRateByChannel.put(
              new Tuple2<>(ChannelTypeEnum.DISPLAY.getValue(), ActionTypeEnum.SERVE.getValue()),
              getRuntimeContext().getMetricGroup().meter("numUTPDisplayServeRecordsInRate",
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      numUTPImkRecordsInRateByChannel.put(
              new Tuple2<>(ChannelTypeEnum.SOCIAL.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().meter("numUTPSocialClickRecordsInRate",
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      numUTPImkRecordsInRateByChannel.put(
              new Tuple2<>(ChannelTypeEnum.SEARCH_ENGINE_FREE_LISTINGS.getValue(), ActionTypeEnum.CLICK.getValue()),
              getRuntimeContext().getMetricGroup().meter("numUTPFreeListingsClickRecordsInRate",
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      numUTPImkRecordsInRateByChannel.put(
              new Tuple2<>(ChannelTypeEnum.GENERIC.getValue(), ActionTypeEnum.ROI.getValue()),
              getRuntimeContext().getMetricGroup().meter("numUTPRoiRecordsInRate",
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));

      etlMeters = new HashMap<>();
      etlMeters.put(UTPImkTransformerMetrics.NUM_ERROR_MALFORMED_RATE,
              getRuntimeContext().getMetricGroup().meter(UTPImkTransformerMetrics.NUM_ERROR_MALFORMED_RATE,
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      etlMeters.put(UTPImkTransformerMetrics.NUM_ERROR_GET_USER_QUERY_RATE,
              getRuntimeContext().getMetricGroup().meter(UTPImkTransformerMetrics.NUM_ERROR_GET_USER_QUERY_RATE,
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      etlMeters.put(UTPImkTransformerMetrics.NUM_ERROR_GET_PARAM_FROM_QUERY_RATE,
              getRuntimeContext().getMetricGroup().meter(UTPImkTransformerMetrics.NUM_ERROR_GET_PARAM_FROM_QUERY_RATE,
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      etlMeters.put(UTPImkTransformerMetrics.NUM_ERROR_GET_PERF_TRACK_NAME_VALUE_RATE,
              getRuntimeContext().getMetricGroup().meter(UTPImkTransformerMetrics.NUM_ERROR_GET_PERF_TRACK_NAME_VALUE_RATE,
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      etlMeters.put(UTPImkTransformerMetrics.NUM_ERROR_PARSE_MT_ID_RATE,
              getRuntimeContext().getMetricGroup().meter(UTPImkTransformerMetrics.NUM_ERROR_PARSE_MT_ID_RATE,
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      etlMeters.put(UTPImkTransformerMetrics.NUM_ERROR_GET_PARAM_VALUE_FROM_QUERY_RATE,
              getRuntimeContext().getMetricGroup().meter(UTPImkTransformerMetrics.NUM_ERROR_GET_PARAM_VALUE_FROM_QUERY_RATE,
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
      etlMeters.put(UTPImkTransformerMetrics.NUM_ERROR_PARSE_CLIENT_ID_RATE,
              getRuntimeContext().getMetricGroup().meter(UTPImkTransformerMetrics.NUM_ERROR_PARSE_CLIENT_ID_RATE,
                      new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
    }

    @Override
    public UnifiedTrackingImkMessage map(UnifiedTrackingRheosMessage sourceRecord) throws Exception {
      String channelType = sourceRecord.getChannelType();
      String actionType = sourceRecord.getActionType();

      long currentTimeMillis = System.currentTimeMillis();
      long latency = currentTimeMillis - sourceRecord.getEventTs();

      this.latency.update(latency);

      Histogram histogram = latencyByChannel.get(new Tuple2<>(channelType, actionType));
      if (histogram != null) {
        histogram.update(latency);
      }

      Meter meter = numUTPImkRecordsInRateByChannel.get(new Tuple2<>(channelType, actionType));
      if (meter != null) {
        meter.markEvent();
      }

      UTPImkTransformer transformer;
      if (actionType.equals(ActionTypeEnum.ROI.getValue())) {
        transformer = new UTPRoiTransformer(sourceRecord, channelType, actionType, etlMeters);
      } else {
        transformer = new UTPImkTransformer(sourceRecord, channelType, actionType, etlMeters);
      }

      UnifiedTrackingImkMessage message = new UnifiedTrackingImkMessage();
      transformer.transform(message);
      message.setRheosHeader(getRheosHeader(currentTimeMillis));
      return message;
    }

    private RheosHeader getRheosHeader(long currentTimeMillis) {
      RheosHeader rheosHeader = new RheosHeader();
      rheosHeader.setEventCreateTimestamp(currentTimeMillis);
      rheosHeader.setEventSentTimestamp(currentTimeMillis);
      rheosHeader.setProducerId(producer);
      rheosHeader.setSchemaId(schemaId);
      return rheosHeader;
    }
  }

  private static class GetRvrId implements KeySelector<UnifiedTrackingImkMessage, Long> {
    @Override
    public Long getKey(UnifiedTrackingImkMessage unifiedTrackingImkMessage) throws Exception {
      return unifiedTrackingImkMessage.getRvrId();
    }
  }
}
