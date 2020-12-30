package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ImkTrckngEventWideMessage;
import com.ebay.app.raptor.chocolate.avro.RheosHeader;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV5;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.provider.AsyncDataRequest;
import com.ebay.traffic.chocolate.flink.nrt.transformer.BaseTransformer;
import com.ebay.traffic.chocolate.flink.nrt.transformer.TransformerFactory;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
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
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Receive messages from filter topics, apply IMK ETL and send messages to another topic.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class ImkTrckngEventTransformApp
    extends AbstractRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImkTrckngEventTransformApp.class);

  /**
   * App entrance
   *
   * @param args input args
   */
  public static void main(String[] args) throws Exception {
    ImkTrckngEventTransformApp transformApp = new ImkTrckngEventTransformApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.IMK_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.IMK_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected String getProducerTopic() {
    return StringConstants.EMPTY;
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(),
        new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected FlinkKafkaProducer<Tuple3<String, Long, byte[]>> getKafkaProducer() {
    return new FlinkKafkaProducer<>(getProducerTopic(), new DefaultKafkaSerializationSchema(),
        getProducerProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
  }

  @Override
  protected Properties getProducerProperties() {
    return PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.IMK_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
  }

  @Override
  protected DataStream<Tuple3<String, Long, byte[]>> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    SingleOutputStreamOperator<FilterMessageV5> filterMessageStream = dataStreamSource.map(new DecodeFilterMessageFunction());
    SingleOutputStreamOperator<FilterMessageV5> filter = filterMessageStream.filter(new FilterEbaySites());
    DataStream<FilterMessageV5> resultStream =
            AsyncDataStream.unorderedWait(filter, new AsyncDataRequest(), 10000, TimeUnit.MILLISECONDS, 100);
    return resultStream.map(new TransformFunction());
  }

  /**
   * Decode Kafka message to filter message
   */
  protected static class DecodeFilterMessageFunction
          extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, FilterMessageV5> {
    private transient Schema rheosHeaderSchema;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
    }

    @Override
    public FilterMessageV5 map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      return FilterMessage.decodeRheos(rheosHeaderSchema, consumerRecord.value());
    }
  }

  /**
   * Apply ETL on filter message
   */
  protected static class TransformFunction
      extends ESMetricsCompatibleRichMapFunction<FilterMessageV5, Tuple3<String, Long, byte[]>> {
    private String rheosProducer;
    private String imkEventWideMessageTopic;
    private int imkEventWideMessageSchemaId;
    private transient EncoderFactory encoderFactory;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      encoderFactory = EncoderFactory.get();
      Properties topicProperties = PropertyMgr.getInstance()
          .loadProperty(PropertyConstants.IMK_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_TOPIC_PROPERTIES);
      rheosProducer = topicProperties.getProperty(PropertyConstants.RHEOS_PRODUCER);
      imkEventWideMessageTopic = topicProperties.getProperty(PropertyConstants.TOPIC);
      imkEventWideMessageSchemaId = Integer
              .parseInt(topicProperties.getProperty(PropertyConstants.SCHEMA_ID_IMK_TRCKNG_EVENT_WDIE));
    }

    @Override
    public Tuple3<String, Long, byte[]> map(FilterMessageV5 filterMessage) throws Exception {
      long currentTimeMillis = System.currentTimeMillis();

      BaseTransformer concreteTransformer = TransformerFactory.getConcreteTransformer(filterMessage);
      ImkTrckngEventWideMessage imkTrckngEventWideMessage = new ImkTrckngEventWideMessage();
      imkTrckngEventWideMessage.setRheosHeader(getRheosHeader(currentTimeMillis, imkEventWideMessageSchemaId));
      concreteTransformer.transform(imkTrckngEventWideMessage);

      return new Tuple3<>(imkEventWideMessageTopic, imkTrckngEventWideMessage.getRvrId(),
              serializeRheosEvent(getRheosEvent(imkTrckngEventWideMessage)));
    }

    private RheosEvent getRheosEvent(GenericRecord v) {
      return new RheosEvent(v);
    }

    private RheosHeader getRheosHeader(long currentTimeMillis, int schemaId) {
      RheosHeader rheosHeader = new RheosHeader();
      rheosHeader.setEventCreateTimestamp(currentTimeMillis);
      rheosHeader.setEventSentTimestamp(currentTimeMillis);
      rheosHeader.setProducerId(rheosProducer);
      rheosHeader.setSchemaId(schemaId);
      return rheosHeader;
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

  @SuppressWarnings("unchecked")
  protected static class FilterEbaySites extends RichFilterFunction<FilterMessageV5> {

    public static final String NUM_RECORDS_IN_COUNTER = "NumRecords";
    public static final String NUM_CLICK_IN_COUNTER = "NumClickRecords";
    public static final String NUM_AR_IN_COUNTER = "NumArRecords";
    public static final String LANDING_PAGE = "LandingPageTypes";

    public static final String NUM_RECORDS_IN_RATE = "NumRecordsInResolveRate";
    public static final String CLICK_RATE = "ClickRate";
    public static final String AR_RATE = "ArRate";


    public static final String INTERNAL_DOMAIN_COUNTER = "ebaySitesReferer";
    public static final String RECORD_LATENCY_GAUGE = "LatencyMetric";
    public static final String PAGE_TYPE = "page_type";
    public static final String SLASH_I = "i";
    public static final String SLASH_ITM = "itm";
    public static final String SLASH_P = "p";
    public static final String SLASH_B = "b";
    public static final String PAGE_E = "e";
    public static final String PAGE_SCH = "sch";
    public static final String PAGE_DEALS = "deals";
    public static final String PAGE_HOME = "home";
    public static final String LATENCY_METER = "latencyMeter";
    public static final String LATENCY_COUTER = "latencyCounter";


    private transient Counter recordCounter;
    private transient Counter clickCounter;
    private transient Counter arCounter;
    private transient Counter latencyCounter;
    private transient Counter landingPageCounter1;
    private transient Counter landingPageCounter2;
    private transient Counter landingPageCounter3;
    private transient Counter landingPageCounter4;
    private transient Counter landingPageCounter5;
    private transient Counter landingPageCounter6;
    private transient Counter landingPageCounter7;
    private transient Counter landingPageCounter8;
    private transient Meter recordRate;
    private transient Meter clickRate;
    private transient Meter arRate;
    private transient Meter latencyMeter;
    private transient Counter internalDomainCounter;
    private transient Gauge recordLatency;

    private transient Pattern ebaySites;

    private transient SherlockioMetrics sherlockioMetrics;

    @Override
    public void open(Configuration config) {
      // Counter
      recordCounter = getRuntimeContext().getMetricGroup().counter(NUM_RECORDS_IN_COUNTER);
      clickCounter = getRuntimeContext().getMetricGroup().counter(NUM_CLICK_IN_COUNTER);
      arCounter = getRuntimeContext().getMetricGroup().counter(NUM_AR_IN_COUNTER);
      latencyCounter = getRuntimeContext().getMetricGroup().counter(LATENCY_COUTER);
      landingPageCounter1 = getRuntimeContext().getMetricGroup().addGroup(SLASH_I).counter(LANDING_PAGE);
      landingPageCounter2 = getRuntimeContext().getMetricGroup().addGroup(SLASH_ITM).counter(LANDING_PAGE);
      landingPageCounter3 = getRuntimeContext().getMetricGroup().addGroup(SLASH_P).counter(LANDING_PAGE);
      landingPageCounter4 = getRuntimeContext().getMetricGroup().addGroup(SLASH_B).counter(LANDING_PAGE);
      landingPageCounter5 = getRuntimeContext().getMetricGroup().addGroup(PAGE_E).counter(LANDING_PAGE);
      landingPageCounter6 = getRuntimeContext().getMetricGroup().addGroup(PAGE_SCH).counter(LANDING_PAGE);
      landingPageCounter7 = getRuntimeContext().getMetricGroup().addGroup(PAGE_DEALS).counter(LANDING_PAGE);
      landingPageCounter8 = getRuntimeContext().getMetricGroup().addGroup(PAGE_HOME).counter(LANDING_PAGE);
      // Meter
      recordRate = getRuntimeContext().getMetricGroup().meter(NUM_RECORDS_IN_RATE, new MeterView(recordCounter, 1));
      clickRate = getRuntimeContext().getMetricGroup().meter(CLICK_RATE, new MeterView(clickCounter, 1));
      arRate = getRuntimeContext().getMetricGroup().meter(AR_RATE, new MeterView(arCounter, 1));
      latencyMeter = getRuntimeContext().getMetricGroup().meter(LATENCY_METER, new MeterView(latencyCounter, 1));

      internalDomainCounter = getRuntimeContext().getMetricGroup().counter(INTERNAL_DOMAIN_COUNTER);
      this.ebaySites = Pattern.compile("^(http[s]?:\\/\\/)?([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);

      Properties properties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
      SherlockioMetrics.init(properties.getProperty(PropertyConstants.SHERLOCKIO_NAMESPACE),
              properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT), properties.getProperty(PropertyConstants.SHERLOCKIO_USER));
      sherlockioMetrics = SherlockioMetrics.getInstance();
      sherlockioMetrics.setJobName("ImkTrckngEventTransformApp");
    }

    @Override
    public boolean filter(FilterMessageV5 value) throws Exception {
      recordCounter.inc();
      recordRate.markEvent();
      switch (value.getChannelAction()) {
        case CLICK:
          clickCounter.inc();
          clickRate.markEvent();
          break;
        case SERVE:
          arCounter.inc();
          arRate.markEvent();
          break;
        default:
          break;
      }

      switch(getPageType(value)) {
        case SLASH_I:
          landingPageCounter1.inc();
          break;
        case SLASH_ITM:
          landingPageCounter2.inc();
          break;
        case SLASH_P:
          landingPageCounter3.inc();
          break;
        case SLASH_B:
          landingPageCounter4.inc();
          break;
        case PAGE_E:
          landingPageCounter5.inc();
          break;
        case PAGE_SCH:
          landingPageCounter6.inc();
          break;
        case PAGE_DEALS:
          landingPageCounter7.inc();
          break;
        case PAGE_HOME:
          landingPageCounter8.inc();
          break;
        default:
          break;
      }
      long currentTimeMillis = System.currentTimeMillis();
      latencyCounter.inc(currentTimeMillis - value.getTimestamp());
      if (value.getChannelType() == ChannelType.ROI) {
        return true;
      }

      if (value.getChannelType() == ChannelType.DISPLAY) {
        if (StringUtils.isNotEmpty(value.getReferer()) && value.getReferer().startsWith("https://ebay.mtag.io/")) {
          LOGGER.info("receive mtag");
          sherlockioMetrics.meter("mtag", 1, Field.of(TransformerConstants.CHANNEL_TYPE, value.getChannelType()), Field.of(TransformerConstants.CHANNEL_ACTION, value.getChannelAction()));
          return true;
        }
        if (StringUtils.isNotEmpty(value.getReferer()) && value.getReferer().startsWith("https://ebay.pissedconsumer.com/")) {
          LOGGER.info("receive pissedconsumer");
          sherlockioMetrics.meter("pissedconsumer", 1, Field.of(TransformerConstants.CHANNEL_TYPE, value.getChannelType()), Field.of(TransformerConstants.CHANNEL_ACTION, value.getChannelAction()));
          return true;
        }
      }

      if (ebaySites.matcher(value.getReferer()).find()) {
        internalDomainCounter.inc();
        return false;
      }

      return true;
    }

    private String getPageType(FilterMessageV5 value) {
      String landingPageType = null;
      UriComponents uriComponents;
      uriComponents = UriComponentsBuilder.fromUriString(value.getUri()).build();
      List<String> pathSegments = uriComponents.getPathSegments();
      if (pathSegments == null || pathSegments.size() == 0) {
        landingPageType = "home";
      } else {
        landingPageType = pathSegments.get(0);
      }
      return landingPageType.toLowerCase();
    }
  }
}
