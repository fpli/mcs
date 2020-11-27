package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.util.GenericRecordUtils;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.flink.nrt.util.UDF;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.*;

/**
 * Receive messages from bot topic, and send rover clicks and opens of site_email and marketing_email to unified tracking topic.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class UTPRoverEventTransformApp
        extends AbstractRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(UTPRoverEventTransformApp.class);

  private static final String USER_ID = "userId";
  private static final int PAGE_ID_ROVER_CLICK = 3084;
  private static final int PAGE_ID_EMAIL_OPEN = 3962;
  private static final String PAGE_NAME_ROVER_EMAIL_OPEN = "roveropen";
  private static final long DEFAULT_SNAPSHOT_ID = -1L;
  public static final String SITE_EMAIL_CHANNEL_ID = "7";
  public static final String MRKT_EMAIL_CHANNEL_ID = "8";
  public static final String EUID = "euid";
  public static final String BEHAVIOR_PULSAR_MISC_BOT = "behavior.pulsar.misc.bot";
  public static final String UC = "uc";
  public static final String REMOTE_IP = "RemoteIP";
  public static final String APP_VERSION = "appVersion";
  public static final String APP_ID = "appId";
  public static final String ENRICHED_OS_VERSION = "enrichedOsVersion";
  public static final String OS_FAMILY = "osFamily";
  public static final String BROWSER_FAMILY = "browserFamily";
  public static final String BROWSER_VERSION = "browserVersion";
  public static final String DEVICE_TYPE = "deviceType";
  public static final String DEVICE_FAMILY = "deviceFamily";
  public static final String REFERER = "Referer";
  public static final String AGENT = "Agent";
  public static final String EMSID = "emsid";
  public static final String SEGNAME = "segname";
  public static final String SID = "sid";
  public static final String UDID = "udid";
  public static final String GADID = "gadid";
  public static final String IDFA = "idfa";
  public static final String EMID = "emid";
  public static final String ADCAMPPU = "adcamppu";
  public static final String GUID = "guid";

  public static void main(String[] args) throws Exception {
    UTPRoverEventTransformApp transformApp = new UTPRoverEventTransformApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return Arrays.asList(PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UTP_ROVER_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
            .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.UTP_ROVER_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
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
            .loadProperty(PropertyConstants.UTP_ROVER_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
  }

  @Override
  protected DataStream<Tuple3<String, Long, byte[]>> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.flatMap(new TransformFlatMapFunction());
  }

  protected static class TransformFlatMapFunction extends RichFlatMapFunction<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {
    private transient GenericRecordDomainDataDecoder decoder;
    private transient RheosEventDeserializer deserializer;
    private transient EncoderFactory encoderFactory;
    private int schemaId;
    private transient Schema schema;
    private String producer;
    private String topic;

    @Override
    public void open(Configuration parameters) throws Exception {
      deserializer = new RheosEventDeserializer();
      Map<String, Object> config = new HashMap<>();
      Properties consumerProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UTP_ROVER_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
      String rheosServiceUrl = consumerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      decoder = new GenericRecordDomainDataDecoder(config);
      encoderFactory = EncoderFactory.get();
      Properties producerProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UTP_ROVER_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
              new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
      String schemaName = (String) producerProperties.get(RheosConstants.RHEOS_TOPIC_SCHEMA);
      schemaId = serializerHelper.getSchemaId(schemaName);
      schema = serializerHelper.getSchema(schemaName);
      producer = (String) producerProperties.get(RheosConstants.RHEOS_PRODUCER);
      Properties topicProperties = PropertyMgr.getInstance()
              .loadProperty(PropertyConstants.UTP_ROVER_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_TOPIC_PROPERTIES);
      topic = topicProperties.getProperty(PropertyConstants.TOPIC);
    }

    @Override
    public void flatMap(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Tuple3<String, Long, byte[]>> out) throws Exception {
      RheosEvent sourceRheosEvent = deserializer.deserialize(topic, consumerRecord.value());
      GenericRecord sourceRecord = decoder.decode(sourceRheosEvent);
      Integer pageId = (Integer) sourceRecord.get(TransformerConstants.PAGE_ID);
      if (pageId == null) {
        return;
      }
      if (pageId != PAGE_ID_ROVER_CLICK && pageId != PAGE_ID_EMAIL_OPEN) {
        return;
      }
      Utf8 urlQueryString = (Utf8) sourceRecord.get(TransformerConstants.URL_QUERY_STRING);
      if (urlQueryString == null) {
        return;
      }
      String channelId = UDF.parseChannelId(urlQueryString.toString());
      if (StringUtils.isEmpty(channelId)) {
        return;
      }

      ChannelTypeEnum channelType;
      if (channelId.equals(SITE_EMAIL_CHANNEL_ID)) {
        channelType = ChannelTypeEnum.SITE_EMAIL;
      } else if (channelId.equals(MRKT_EMAIL_CHANNEL_ID)) {
        channelType = ChannelTypeEnum.MRKT_EMAIL;
      } else {
        return;
      }

      Utf8 pageName = (Utf8) sourceRecord.get(TransformerConstants.PAGE_NAME);
      if (pageName == null) {
        return;
      }
      if (pageId == PAGE_ID_EMAIL_OPEN && !PAGE_NAME_ROVER_EMAIL_OPEN.equals(pageName.toString())) {
        return;
      }

      ActionTypeEnum actionType = pageId == PAGE_ID_ROVER_CLICK ? ActionTypeEnum.CLICK : ActionTypeEnum.OPEN;
      Map<String, String> applicationPayload = GenericRecordUtils.getMap(sourceRecord, TransformerConstants.APPLICATION_PAYLOAD);
      Map<String, String> clientData = GenericRecordUtils.getMap(sourceRecord, TransformerConstants.CLIENT_DATA);

      UnifiedTrackingRheosMessage.Builder builder = UnifiedTrackingRheosMessage.newBuilder();

      builder.setEventId(UUID.randomUUID().toString());
      if (channelType == ChannelTypeEnum.SITE_EMAIL) {
        builder.setProducerEventId(applicationPayload.getOrDefault(EUID, StringConstants.EMPTY));
      } else {
        builder.setProducerEventId(StringConstants.EMPTY);
      }

      Long eventTimestamp = (Long) sourceRecord.get(TransformerConstants.EVENT_TIMESTAMP);
      builder.setEventTs(eventTimestamp);
      builder.setProducerEventTs(eventTimestamp);
      builder.setRlogId(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.RLOGID));
      builder.setTrackingId(StringConstants.EMPTY);
      Utf8 userId = (Utf8) sourceRecord.get(USER_ID);
      if (userId != null) {
        Long parse = Longs.tryParse(userId.toString());
        if (parse != null) {
          builder.setUserId(parse);
        } else {
          builder.setUserId(0L);
        }
      } else {
        builder.setUserId(0L);
      }

      String adcamppu = applicationPayload.get(ADCAMPPU);
      if (StringUtils.isNotEmpty(adcamppu)) {
        builder.setPublicUserId(adcamppu.trim());
      } else {
        builder.setPublicUserId(StringConstants.EMPTY);
      }

      String emid = applicationPayload.get(EMID);
      if (emid == null) {
        builder.setEncryptedUserId(0L);
      } else {
        Long parse = Longs.tryParse(emid);
        if (parse != null) {
          builder.setEncryptedUserId(parse);
        } else {
          builder.setEncryptedUserId(0L);
        }
      }

      Utf8 guid = (Utf8) sourceRecord.get(GUID);
      builder.setGuid(guid.toString());
      builder.setIdfa(applicationPayload.getOrDefault(IDFA, StringConstants.EMPTY));
      builder.setGadid(applicationPayload.getOrDefault(GADID, StringConstants.EMPTY));
      builder.setDeviceId(applicationPayload.getOrDefault(UDID, StringConstants.EMPTY));

      builder.setChannelType(channelType.getValue());
      builder.setActionType(actionType.getValue());
      builder.setPartner(StringConstants.EMPTY);

      if (channelType == ChannelTypeEnum.MRKT_EMAIL) {
        builder.setCampaignId(applicationPayload.getOrDefault(SEGNAME, StringConstants.EMPTY));
      } else {
        String emsid = UDF.substring(applicationPayload.get(EMSID), "e", ".mle");
        String campaignId = emid != null ? emsid : UDF.substring(applicationPayload.get(SID), "e", ".mle");
        if (campaignId == null) {
          builder.setCampaignId(StringConstants.EMPTY);
        } else {
          builder.setCampaignId(campaignId);
        }
      }

      builder.setRotationId(StringConstants.EMPTY);
      Utf8 siteId = (Utf8) sourceRecord.get("siteId");
      if (siteId != null) {
        Integer parse = Ints.tryParse(siteId.toString());
        if (parse != null) {
          builder.setSiteId(parse);
        } else {
          builder.setSiteId(0);
        }
      } else {
        builder.setSiteId(0);
      }
      builder.setUrl(urlQueryString.toString());
      builder.setReferer(clientData.getOrDefault(REFERER, StringConstants.EMPTY));
      builder.setUserAgent(clientData.getOrDefault(AGENT, StringConstants.EMPTY));
      builder.setDeviceFamily(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, DEVICE_FAMILY));
      builder.setDeviceType(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, DEVICE_TYPE));
      builder.setBrowserVersion(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, BROWSER_VERSION));
      builder.setBrowserFamily(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, BROWSER_FAMILY));
      builder.setOsFamily(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, OS_FAMILY));
      builder.setOsVersion(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, ENRICHED_OS_VERSION));
      builder.setAppId(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, APP_ID));
      builder.setAppVersion(GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, APP_VERSION));
      builder.setService(StringConstants.EMPTY);
      builder.setServer(System.getenv("HOSTNAME"));
      builder.setRemoteIp(clientData.getOrDefault(REMOTE_IP, StringConstants.EMPTY));
      builder.setPageId(pageId);
      builder.setIsBot(consumerRecord.topic().equals(BEHAVIOR_PULSAR_MISC_BOT));
      int geoId = 0;
      if (applicationPayload.containsKey(UC)) {
        Integer integer = Ints.tryParse(applicationPayload.get(UC));
        geoId = integer == null ? 0 : integer;
      }
      builder.setGeoId(geoId);
      builder.setPayload(applicationPayload);
      RheosEvent rheosEvent = getRheosEvent(builder.build());
      out.collect(new Tuple3<>(this.topic, DEFAULT_SNAPSHOT_ID, serializeRheosEvent(rheosEvent)));
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

  }

}
