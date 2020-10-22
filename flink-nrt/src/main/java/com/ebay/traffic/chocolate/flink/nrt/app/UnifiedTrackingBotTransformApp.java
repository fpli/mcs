package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
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
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
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
    return dataStreamSource.map(new DecodeFunction()).filter(new FilterFunction()).map(new TransformFunction());
  }

  /**
   * Decode Kafka message to generic record
   */
  protected static class DecodeFunction extends RichMapFunction<ConsumerRecord<byte[], byte[]>, GenericRecord> {
    private transient GenericRecordDomainDataDecoder decoder;
    private transient RheosEventDeserializer deserializer;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      deserializer = new RheosEventDeserializer();
      Map<String, Object> config = new HashMap<>();
      Properties properties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
      String rheosServiceUrl = properties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      decoder = new GenericRecordDomainDataDecoder(config);
    }

    @Override
    public GenericRecord map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      RheosEvent rheosEvent = deserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
      return decoder.decode(rheosEvent);
    }
  }

  /**
   * Currently, we only need click and email-opens of site_email and marketing_email
   */
  protected static class FilterFunction extends RichFilterFunction<GenericRecord> {
    @Override
    public boolean filter(GenericRecord sourceRecord) throws Exception {
      if (sourceRecord.get(TransformerConstants.PAGE_ID) == null) {
        return false;
      }
      int pageId = (int) sourceRecord.get(TransformerConstants.PAGE_ID);
      String pageName = getField(sourceRecord, TransformerConstants.PAGE_NAME, null);
      ChannelIdEnum channelType = parseChannelType(sourceRecord);
      // only need site_email and marketing_email events
      if (ChannelIdEnum.SITE_EMAIL != channelType && ChannelIdEnum.MRKT_EMAIL != channelType) {
        return false;
      }
      // only need rover click
      if (pageId == PAGE_ID_ROVER_CLICK) {
        return true;
      }
      // only need rover email open
      return pageId == PAGE_ID_EMAIL_OPEN && PAGE_NAME_ROVER_EMAIL_OPEN.equals(pageName);
    }
  }

  /**
   * Convert generic record to unified tracking message
   */
  protected static class TransformFunction extends RichMapFunction<GenericRecord, Tuple3<String, Long, byte[]>> {
    private transient EncoderFactory encoderFactory;
    private int schemaId;
    private transient Schema schema;
    private String producer;
    private String topic;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      encoderFactory = EncoderFactory.get();
      Map<String, Object> config = new HashMap<>();
      Properties producerProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UNIFIED_TRACKING_BOT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
      String rheosServiceUrl = producerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
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
    }

    @Override
    public Tuple3<String, Long, byte[]> map(GenericRecord sourceRecord) throws Exception {
      int pageId = (int) sourceRecord.get(TransformerConstants.PAGE_ID);
      String pageName = getField(sourceRecord, TransformerConstants.PAGE_NAME, null);
      ChannelIdEnum channelType = parseChannelType(sourceRecord);
      String channelTypeStr = Objects.requireNonNull(channelType).getLogicalChannel().getAvro().name();
      String channelActionStr = pageId == PAGE_ID_ROVER_CLICK ? ChannelAction.CLICK.name() : ChannelAction.EMAIL_OPEN.name();
      BehaviorMessage behaviorMessage = buildMessage(sourceRecord, pageId, pageName, channelActionStr, channelTypeStr);
      RheosEvent rheosEvent = getRheosEvent(behaviorMessage);
      return new Tuple3<>(topic, DEFAULT_SNAPSHOT_ID, serializeRheosEvent(rheosEvent));
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
    protected BehaviorMessage buildMessage(GenericRecord genericRecord, Integer pageId, String pageName, String channelAction, String channelType) {
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
      // directly get from urlQueryString is not correct. should get from applicationPayload
      String urlQueryString = applicationPayload.get(TransformerConstants.URL_QUERY_STRING);
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
