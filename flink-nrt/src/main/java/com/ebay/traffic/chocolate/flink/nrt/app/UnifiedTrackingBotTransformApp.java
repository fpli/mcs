package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.flink.nrt.function.SherlockioMetricsCompatibleRichFlatMapFunction;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Receive messages from bot topic, and send rover clicks and opens of site_email and marketing_email to unified tracking topic.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class UnifiedTrackingBotTransformApp
        extends AbstractRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedTrackingBotTransformApp.class);

  private static final String USER_ID = "userId";
  private static final String REMOTE_IP = "remoteIP";
  private static final int PAGE_ID_ROVER_CLICK = 3084;
  private static final int PAGE_ID_EMAIL_OPEN = 3962;
  private static final String PAGE_NAME_ROVER_EMAIL_OPEN = "roveropen";
  private static final long DEFAULT_SNAPSHOT_ID = -1L;
  private static final String PAGE_NAME_ROVER_CLICK_BOT = "Rover_Click_Bot";
  private static final String PAGE_NAME_ROVER_OPEN_BOT = "Rover_Open_Bot";
  private static final String INVALID_INCOMING = "InvalidIncoming";
  private static final String VALID_INCOMING = "ValidIncoming";
  private static final String BOT_EVENT_LATENCY = "BotEventLatency";
  private static final String BOT_EVENT_LATENCY_HISTOGRAM = "BotEventLatencyHistogram";
  private static final String EMPTY_PAGE_ID = "EmptyPageID";
  private static final String OTHER_CHANNEL_EVENTS = "OtherChannelEvents";


  public static void main(String[] args) throws Exception {
    UnifiedTrackingBotTransformApp transformApp = new UnifiedTrackingBotTransformApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return Arrays.asList(PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
            .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
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

  /**
   * All the snapshot_ids are set as {@link #DEFAULT_SNAPSHOT_ID}, so we need to use
   * {@link com.ebay.traffic.chocolate.flink.nrt.kafka.RoundRobinPartitioner}
   * @return producer properties
   */
  @Override
  protected Properties getProducerProperties() {
    return PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
  }

  @Override
  protected DataStream<Tuple3<String, Long, byte[]>> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.flatMap(new TransformFlatMapFunction());
  }

  protected static class TransformFlatMapFunction extends SherlockioMetricsCompatibleRichFlatMapFunction<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {
    private transient GenericRecordDomainDataDecoder decoder;
    private transient RheosEventDeserializer deserializer;
    private transient EncoderFactory encoderFactory;
    private int schemaId;
    private transient Schema schema;
    private String producer;
    private String topic;
    private SherlockioMetrics sherlockioMetrics;


    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      deserializer = new RheosEventDeserializer();
      Map<String, Object> config = new HashMap<>();
      Properties consumerProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
      String rheosServiceUrl = consumerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      decoder = new GenericRecordDomainDataDecoder(config);
      encoderFactory = EncoderFactory.get();
      Properties producerProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
              new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
      String schemaName = (String) producerProperties.get(RheosConstants.RHEOS_TOPIC_SCHEMA);
      schemaId = serializerHelper.getSchemaId(schemaName);
      schema = serializerHelper.getSchema(schemaName);
      producer = (String) producerProperties.get(RheosConstants.RHEOS_PRODUCER);
      Properties topicProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_PRODUCER_TOPIC_PROPERTIES);
      topic = topicProperties.getProperty(PropertyConstants.TOPIC);
      sherlockioMetrics = SherlockioMetrics.getInstance();
      sherlockioMetrics.setJobName(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_JOBNAME);
    }

    @Override
    public void flatMap(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Tuple3<String, Long, byte[]>> out) throws Exception {
      Headers headers = consumerRecord.headers();
      String schemaVersion = StringConstants.EMPTY;
      if (headers != null) {
        for (Header header : headers) {
          if ("schemaVersion".equals(header.key())) {
            schemaVersion = new String(header.value());
          }
        }
      }
      RheosEvent sourceRheosEvent = deserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
      GenericRecord sourceRecord = decoder.decode(sourceRheosEvent);
      boolean isValid = filter(sourceRecord);
      if (!isValid) {
        sherlockioMetrics.meter(INVALID_INCOMING, 1);
        return;
      }
      int pageId = (int) sourceRecord.get(TransformerConstants.PAGE_ID);
      String pageName;
      String channelActionStr;
      if (pageId == PAGE_ID_ROVER_CLICK) {
        pageName = PAGE_NAME_ROVER_CLICK_BOT;
        channelActionStr = ChannelAction.CLICK.name();
      } else {
        pageName = PAGE_NAME_ROVER_OPEN_BOT;
        channelActionStr = ChannelAction.EMAIL_OPEN.name();
      }
      ChannelIdEnum channelType = parseChannelType(sourceRecord);
      String channelTypeStr = Objects.requireNonNull(channelType).getLogicalChannel().getAvro().name();
      sherlockioMetrics.meter(VALID_INCOMING, 1, Field.of(TransformerConstants.PAGE_ID, pageId), Field.of(TransformerConstants.PAGE_NAME, pageName), Field.of(TransformerConstants.CHANNEL_TYPE, channelTypeStr), Field.of(TransformerConstants.CHANNEL_ACTION, channelActionStr));
      Long currentTimestamp = System.currentTimeMillis();
      BehaviorMessage behaviorMessage = buildMessage(sourceRecord, pageId, pageName, channelActionStr, channelTypeStr, schemaVersion);
      sherlockioMetrics.mean(BOT_EVENT_LATENCY, currentTimestamp - behaviorMessage.getEventTimestamp(), Field.of(TransformerConstants.CHANNEL_TYPE, channelTypeStr), Field.of(TransformerConstants.CHANNEL_ACTION, channelActionStr));
      sherlockioMetrics.meanByHistogram(BOT_EVENT_LATENCY_HISTOGRAM, currentTimestamp - behaviorMessage.getEventTimestamp(), Field.of(TransformerConstants.CHANNEL_TYPE, channelTypeStr), Field.of(TransformerConstants.CHANNEL_ACTION, channelActionStr));
      RheosEvent rheosEvent = getRheosEvent(behaviorMessage);
      out.collect(new Tuple3<>(topic, DEFAULT_SNAPSHOT_ID, serializeRheosEvent(rheosEvent)));
    }

    /**
     * Currently, we only need click and email-opens of site_email and marketing_email
     * @param sourceRecord bot record
     * @return send to unified tracking or not
     */
    private boolean filter(GenericRecord sourceRecord) {
      long borderTs = 1624612800000L;
      if ((Long) sourceRecord.get(TransformerConstants.EVENT_TIMESTAMP) < borderTs) {
        return false;
      }
      if (sourceRecord.get(TransformerConstants.PAGE_ID) == null) {
        sherlockioMetrics.meter(EMPTY_PAGE_ID, 1);
        return false;
      }
      int pageId = (int) sourceRecord.get(TransformerConstants.PAGE_ID);
      String pageName = getField(sourceRecord, TransformerConstants.PAGE_NAME, null);
      ChannelIdEnum channelType = parseChannelType(sourceRecord);
      // only need site_email and marketing_email events
      if (ChannelIdEnum.SITE_EMAIL != channelType && ChannelIdEnum.MRKT_EMAIL != channelType) {
        sherlockioMetrics.meter(OTHER_CHANNEL_EVENTS, 1);
        return false;
      }
      // only need rover click
      if (pageId == PAGE_ID_ROVER_CLICK) {
        return true;
      }

      // only need rover email open
      return pageId == PAGE_ID_EMAIL_OPEN && PAGE_NAME_ROVER_EMAIL_OPEN.equals(pageName);
    }

    public RheosEvent getRheosEvent(GenericRecord v) {
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
      return new GenericDatumWriter<>(rheosEvent.getSchema());
    }

    @SuppressWarnings("unchecked")
    protected BehaviorMessage buildMessage(GenericRecord genericRecord, Integer pageId, String pageName, String channelAction, String channelType, String schemaVersion) {
      BehaviorMessage record = new BehaviorMessage();
      Map<String, String> applicationPayload = convertMap(((Map<Utf8, Utf8>) genericRecord.get(TransformerConstants.APPLICATION_PAYLOAD)));
      record.setGuid(String.valueOf(genericRecord.get(TransformerConstants.GUID)));
      record.setAdguid(String.valueOf(genericRecord.get(TransformerConstants.GUID)));
      record.setEventTimestamp((Long) genericRecord.get(TransformerConstants.EVENT_TIMESTAMP));
      record.setSid(getField(genericRecord, TransformerConstants.SID, null));
      record.setPageId(pageId);
      record.setPageName(pageName);
      record.setEventFamily(getField(genericRecord, TransformerConstants.EVENT_FAMILY, null));
      record.setEventAction(getField(genericRecord, TransformerConstants.EVENT_ACTION, null));
      record.setUserId(getField(genericRecord, USER_ID, null));
      record.setSiteId(getField(genericRecord, TransformerConstants.SITE_ID, null));
      record.setSessionId(getField(genericRecord, TransformerConstants.SESSION_ID, null));
      record.setSnapshotId(String.valueOf(DEFAULT_SNAPSHOT_ID));
      record.setSeqNum(getField(genericRecord, TransformerConstants.SEQ_NUM, null));
      record.setRdt((Integer) genericRecord.get(TransformerConstants.RDT));
      record.setRefererHash(getField(genericRecord, TransformerConstants.REFERER_HASH, null));
      // directly get from urlQueryString is not correct. should get from applicationPayload; it be changed in new topic(pulsar v2), we need directly get from urlQueryString
      String urlQueryString;
      if ("2".equals(schemaVersion)) {
        urlQueryString = getField(genericRecord, TransformerConstants.URL_QUERY_STRING, null);
      } else {
        urlQueryString = applicationPayload.get(TransformerConstants.URL_QUERY_STRING);
      }
      record.setUrlQueryString(urlQueryString);
      record.setWebServer(getField(genericRecord, TransformerConstants.WEB_SERVER, null));
      record.setClientIP(getField(genericRecord, TransformerConstants.CLIENT_IP, null));
      record.setRemoteIP(getField(genericRecord, REMOTE_IP, null));
      record.setAgentInfo(getField(genericRecord, TransformerConstants.AGENT_INFO, null));
      record.setAppId(getField(genericRecord, TransformerConstants.APP_ID, null));
      record.setAppVersion(getField(genericRecord, TransformerConstants.APP_VERSION, null));
      record.setOsVersion(getField(genericRecord, TransformerConstants.OS_VERSION, null));
      record.setCobrand(getField(genericRecord, TransformerConstants.COBRAND, null));
      record.setDeviceFamily(getField(genericRecord, TransformerConstants.DEVICE_FAMILY, null));
      record.setDeviceType(getField(genericRecord, TransformerConstants.DEVICE_TYPE, null));
      record.setBrowserVersion(getField(genericRecord, TransformerConstants.BROWSER_VERSION, null));
      record.setBrowserFamily(getField(genericRecord, TransformerConstants.BROWSER_FAMILY, null));
      record.setOsFamily(getField(genericRecord, TransformerConstants.OS_FAMILY, null));
      record.setEnrichedOsVersion(getField(genericRecord, TransformerConstants.ENRICHED_OS_VERSION, null));
      record.setApplicationPayload(applicationPayload);
      record.setRlogid(getField(genericRecord, TransformerConstants.RLOGID, null));
      record.setClientData(convertMap((HashMap<Utf8, Utf8>) genericRecord.get(TransformerConstants.CLIENT_DATA)));
      record.setChannelAction(channelAction);
      record.setChannelType(channelType);
      record.setDispatchId(StringConstants.EMPTY);
      List<Map<String, String>> data = new ArrayList<>();
      record.setData(data);
      return record;
    }

    protected Map<String, String> convertMap(Map<Utf8, Utf8> map) {
      Map<String, String> target = new HashMap<>();
      map.forEach((k, v) -> target.put(String.valueOf(k), String.valueOf(v)));
      return target;
    }
  }

  @SuppressWarnings("unchecked")
  protected static ChannelIdEnum parseChannelType(GenericRecord genericRecord) {
    HashMap<Utf8, Utf8> applicationPayload = ((HashMap<Utf8, Utf8>) genericRecord.get(TransformerConstants.APPLICATION_PAYLOAD));
    if (applicationPayload.containsKey(new Utf8(TransformerConstants.CHNL))) {
      return ChannelIdEnum.parse(String.valueOf(applicationPayload.get(new Utf8(TransformerConstants.CHNL))));
    }

    Utf8 urlQueryString = (Utf8) genericRecord.get(TransformerConstants.URL_QUERY_STRING);
    return urlQueryString == null ? null : parseChannelType(urlQueryString);
  }

  protected static ChannelIdEnum parseChannelType(Utf8 urlQueryString) {
    List<String> strings = URLEncodedUtils.parsePathSegments(String.valueOf(urlQueryString), StandardCharsets.UTF_8);
    if (CollectionUtils.isEmpty(strings)) {
      return null;
    }
    String lastElem = strings.get(strings.size() - 1);
    if (!lastElem.contains(StringConstants.QUESTION_MARK)) {
      return null;
    }
    return ChannelIdEnum.parse(lastElem.substring(0, lastElem.indexOf(StringConstants.QUESTION_MARK)));
  }

  public static String getField(GenericRecord genericRecord, String key, String defaultValue) {
    return genericRecord.get(key) == null ? defaultValue : genericRecord.get(key).toString();
  }
}
