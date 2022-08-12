package com.ebay.traffic.chocolate.flink.nrt.app;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV6;
import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV6;
import com.ebay.traffic.chocolate.flink.nrt.filter.FilterRuleMgr;
import com.ebay.traffic.chocolate.flink.nrt.filter.configs.FilterRuleType;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterContainer;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.provider.publisher.PublisherIdRequest;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumExWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Receive messages from listener topic, apply ETL and send messages to filter topic.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class EPNFilterApp
    extends AbstractRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EPNFilterApp.class);

  public static void main(String[] args) throws Exception {
    EPNFilterApp transformApp = new EPNFilterApp();
    transformApp.run(args);
  }

  @Override
  protected void loadProperty() {
    this.env_config = PropertyMgr.getInstance().loadYaml("epn-filter-app.yaml");
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<String> getConsumerTopics() {
    Map<String, Object> source = (Map<String, Object>) env_config.get(PropertyConstants.SOURCE);
    String topics = (String) source.get(PropertyConstants.TOPIC);
    return Arrays.asList(topics.split(StringConstants.COMMA));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Properties getConsumerProperties() throws IOException {
    Map<String, Object> source = (Map<String, Object>) env_config.get(PropertyConstants.SOURCE);
    Properties consumerProperties = new Properties();
    consumerProperties.load(new StringReader((String) source.get(PropertyConstants.PRORERTIES)));
    return consumerProperties;
  }

  @Override
  protected String getProducerTopic() {
    return StringConstants.EMPTY;
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() throws IOException {
    return new FlinkKafkaConsumer<>(getConsumerTopics(),
            new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected FlinkKafkaProducer<Tuple3<String, Long, byte[]>> getKafkaProducer() throws IOException {
    return new FlinkKafkaProducer<>(getProducerTopic(), new DefaultKafkaSerializationSchema(),
            getProducerProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Properties getProducerProperties() throws IOException {
    Map<String, Object> sink = (Map<String, Object>) env_config.get("sink");
    Properties producerProperties = new Properties();
    producerProperties.load(new StringReader((String) sink.get(PropertyConstants.PRORERTIES)));
    return producerProperties;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStream<Tuple3<String, Long, byte[]>> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    SingleOutputStreamOperator<ListenerMessageV6> stream = dataStreamSource.map(new Deserialize()).name("deserialize").uid("deserialize");

    Map<String, Object> asyncNukv = (Map<String, Object>) env_config.get("asyncNukv");
    String name = (String) asyncNukv.get("name");
    String uid = (String) asyncNukv.get("uid");
    Integer capacity = (Integer) asyncNukv.get("capacity");
    Map<String, Object> timeoutConfig = (Map<String, Object>) asyncNukv.get("timeout");
    Integer timeout = (Integer) timeoutConfig.get("duration");
    TimeUnit timeUnit = TimeUnit.valueOf((String) timeoutConfig.get("timeUnit"));

//    return stream.map(new Process()).name("process").uid("process")
//            .map(new Serialize()).name("serialize").uid("serialize");

    return AsyncDataStream
            .unorderedWait(stream, new PublisherIdRequest(env_config), timeout, timeUnit, capacity).name(name).uid(uid)
            .map(new Process()).name("process").uid("process")
            .map(new Serialize()).name("serialize").uid("serialize");
  }

  protected static class Process
          extends RichMapFunction<ListenerMessageV6, FilterMessageV6> {
    private transient FilterContainer filters;
    private transient Meter numRtRulesErrorRate;

    @Override
    public void open(Configuration parameters) throws Exception {
      FilterRuleMgr.getInstance().initFilterRuleConfig("filter_rule_config.json");
      filters = FilterContainer.createDefault(FilterRuleMgr.getInstance().getFilterRuleConfigMap());

      this.numRtRulesErrorRate = getRuntimeContext().getMetricGroup().meter("numRtRulesErrorRate",
              new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public FilterMessageV6 map(ListenerMessageV6 message) throws Exception {
      FilterMessage outMessage = new FilterMessage();
      outMessage.setSnapshotId(message.getSnapshotId());
      outMessage.setShortSnapshotId(message.getShortSnapshotId());
      outMessage.setTimestamp(message.getTimestamp());
      outMessage.setUserId(message.getUserId());
      outMessage.setCguid(message.getCguid());
      outMessage.setGuid(message.getGuid());
      outMessage.setRemoteIp(message.getRemoteIp());
      outMessage.setLangCd(message.getLangCd());
      outMessage.setUserAgent(message.getUserAgent());
      outMessage.setGeoId(message.getGeoId());
      outMessage.setUdid(message.getUdid());
      outMessage.setReferer(message.getReferer());
      outMessage.setPublisherId(message.getPublisherId());
      outMessage.setCampaignId(message.getCampaignId());
      outMessage.setSiteId(message.getSiteId());
      outMessage.setLandingPageUrl(message.getLandingPageUrl());
      outMessage.setSrcRotationId(message.getSrcRotationId());
      outMessage.setDstRotationId(message.getDstRotationId());
      outMessage.setRequestHeaders(message.getRequestHeaders());
      outMessage.setUri(message.getUri());
      outMessage.setResponseHeaders(message.getResponseHeaders());
      outMessage.setChannelAction(message.getChannelAction());
      outMessage.setChannelType(message.getChannelType());
      outMessage.setHttpMethod(message.getHttpMethod());
      outMessage.setSnid(message.getSnid());
      outMessage.setIsTracked(message.getIsTracked());
      try {
        long rtFilterRules = filters.test(message);
        outMessage.setRtRuleFlags(rtFilterRules);
      } catch (Exception e) {
        outMessage.setRtRuleFlags((long) FilterRuleType.ERROR.getRuleDigitPosition());
        this.numRtRulesErrorRate.markEvent();
      }

      return outMessage;
    }
  }

  protected static class Deserialize
          extends RichMapFunction<ConsumerRecord<byte[], byte[]>, ListenerMessageV6> {
    private transient Schema rheosHeaderSchema;

    private transient Histogram beforeFilterLatency;
    private transient Histogram beforeFilterClickLatency;
    private transient Histogram beforeFilterImpressionLatency;
    private transient Histogram beforeFilterViewableLatency;
    private transient Meter numClickRate;
    private transient Meter numImpressionRate;
    private transient Meter numViewableRate;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();

      this.beforeFilterLatency = getRuntimeContext().getMetricGroup().histogram("beforeFilterLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));
      this.beforeFilterClickLatency = getRuntimeContext().getMetricGroup().histogram("beforeFilterClickLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));
      this.beforeFilterImpressionLatency = getRuntimeContext().getMetricGroup().histogram("beforeFilterImpressionLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));
      this.beforeFilterViewableLatency = getRuntimeContext().getMetricGroup().histogram("beforeFilterViewableLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));

      this.numClickRate = getRuntimeContext().getMetricGroup().meter("numClickRate",
              new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      this.numImpressionRate = getRuntimeContext().getMetricGroup().meter("numImpressionRate",
              new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      this.numViewableRate = getRuntimeContext().getMetricGroup().meter("numViewableRate",
              new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public ListenerMessageV6 map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      ListenerMessage message = ListenerMessage.decodeRheos(rheosHeaderSchema, consumerRecord.value());
      long currentTimeMillis = System.currentTimeMillis();
      long latency = currentTimeMillis - message.getTimestamp();
      this.beforeFilterLatency.update(latency);
      switch (message.getChannelAction()) {
        case CLICK:
          this.beforeFilterClickLatency.update(latency);
          this.numClickRate.markEvent();
          break;
        case IMPRESSION:
          this.beforeFilterImpressionLatency.update(latency);
          this.numImpressionRate.markEvent();
          break;
        case VIEWABLE:
          this.beforeFilterViewableLatency.update(latency);
          this.numViewableRate.markEvent();
          break;
        default:
      }
      return message;
    }
  }

  /**
   * Apply ETL on listener message
   */
  protected static class Serialize extends RichMapFunction<FilterMessageV6, Tuple3<String, Long, byte[]>> {
    private int schemaId;
    private transient Schema schema;
    private String producer;

    private transient EncoderFactory encoderFactory;
    private String topic;

    private transient Histogram afterFilterLatency;
    private transient Histogram afterFilterClickLatency;
    private transient Histogram afterFilterImpressionLatency;
    private transient Histogram afterFilterViewableLatency;

    private transient Meter numClickFilterPassedRate;
    private transient Meter numImpressionFilterPassedRate;
    private transient Meter numViewableFilterPassedRate;

    @SuppressWarnings("unchecked")
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      encoderFactory = EncoderFactory.get();
      Map<String, Object> envConfig = PropertyMgr.getInstance().loadYaml("epn-filter-app.yaml");
      Map<String, Object> sink = (Map<String, Object>) envConfig.get("sink");
      topic = (String) sink.get(PropertyConstants.TOPIC);

      Properties producerProperties = new Properties();
      producerProperties.load(new StringReader((String) sink.get(PropertyConstants.PRORERTIES)));

      Map<String, Object> config = new HashMap<>();
      String rheosServiceUrl = producerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);

      SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
              new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
      String schemaName = (String) producerProperties.get(RheosConstants.RHEOS_TOPIC_SCHEMA);
      schemaId = serializerHelper.getSchemaId(schemaName);
      schema = serializerHelper.getSchema(schemaName);

      producer = (String) producerProperties.get(RheosConstants.RHEOS_PRODUCER);

      this.afterFilterLatency = getRuntimeContext().getMetricGroup().histogram("afterFilterLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));
      this.afterFilterClickLatency = getRuntimeContext().getMetricGroup().histogram("afterFilterClickLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));
      this.afterFilterImpressionLatency = getRuntimeContext().getMetricGroup().histogram("afterFilterImpressionLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));
      this.afterFilterViewableLatency = getRuntimeContext().getMetricGroup().histogram("afterFilterViewableLatency",
              new DropwizardHistogramWrapper(
                      new com.codahale.metrics.Histogram(
                              new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));

      this.numClickFilterPassedRate = getRuntimeContext().getMetricGroup().meter("numClickFilterPassedRate",
              new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      this.numImpressionFilterPassedRate = getRuntimeContext().getMetricGroup().meter("numImpressionFilterPassedRate",
              new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      this.numViewableFilterPassedRate = getRuntimeContext().getMetricGroup().meter("numViewableFilterPassedRate",
              new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public Tuple3<String, Long, byte[]> map(FilterMessageV6 message) throws Exception {
      long currentTimeMillis = System.currentTimeMillis();
      long latency = currentTimeMillis - message.getTimestamp();
      this.afterFilterLatency.update(latency);
      switch (message.getChannelAction()) {
        case CLICK:
          this.afterFilterClickLatency.update(latency);
          break;
        case IMPRESSION:
          this.afterFilterImpressionLatency.update(latency);
          break;
        case VIEWABLE:
          this.afterFilterViewableLatency.update(latency);
          break;
        default:
      }
      if (message.getRtRuleFlags() == 0) {
        switch (message.getChannelAction()) {
          case CLICK:
            this.numClickFilterPassedRate.markEvent();
            break;
          case IMPRESSION:
            this.numImpressionFilterPassedRate.markEvent();
            break;
          case VIEWABLE:
            this.numViewableFilterPassedRate.markEvent();
            break;
          default:
        }
      }
      return new Tuple3<>(this.topic, message.getShortSnapshotId(), serializeRheosEvent(getRheosEvent(message)));
    }

    private RheosEvent getRheosEvent(GenericRecord v) {
      RheosEvent rheosEvent = new RheosEvent(schema);
      long t = System.currentTimeMillis();
      rheosEvent.setEventCreateTimestamp(t);
      rheosEvent.setEventSentTimestamp(t);
      rheosEvent.setSchemaId(schemaId);
      rheosEvent.setProducerId(producer);

      for (Schema.Field field : v.getSchema().getFields()) {
        String fn = field.name();
        Object fv = v.get(fn);
        if (fv != null) {
          rheosEvent.put(fn, fv);
        }
      }
      return rheosEvent;
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
      return new GenericDatumExWriter<>(rheosEvent.getSchema());
    }
  }

}
