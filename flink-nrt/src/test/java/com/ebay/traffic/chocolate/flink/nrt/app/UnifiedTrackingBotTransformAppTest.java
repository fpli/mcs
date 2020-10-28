package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnifiedTrackingBotTransformAppTest {
  private UnifiedTrackingBotTransformApp unifiedTrackingBotTransformApp;

  @Before
  public void setUp() throws Exception {
    unifiedTrackingBotTransformApp = new UnifiedTrackingBotTransformApp();
  }

  @Test
  public void getConsumerTopics() {
    assertEquals(Collections.singletonList("behavior.pulsar.misc.bot"), unifiedTrackingBotTransformApp.getConsumerTopics());
  }

  @Test
  public void getConsumerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-bot-transform");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("receive.buffer.bytes", "65536");
    properties.setProperty("request.timeout.ms", "305000");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    assertEquals(properties, unifiedTrackingBotTransformApp.getConsumerProperties());
  }

  @Test
  public void getProducerTopic() {
    assertEquals("", unifiedTrackingBotTransformApp.getProducerTopic());
  }

  @Test
  public void getKafkaConsumer() {
    FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> kafkaConsumer = unifiedTrackingBotTransformApp.getKafkaConsumer();
    assertEquals("org.apache.kafka.clients.consumer.ConsumerRecord", kafkaConsumer.getProducedType().getTypeClass().getName());
  }

  @Test
  public void getKafkaProducer() {
    FlinkKafkaProducer<Tuple3<String, Long, byte[]>> kafkaProducer = unifiedTrackingBotTransformApp.getKafkaProducer();
    assertEquals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer", kafkaProducer.getClass().getName());
  }

  @Test
  public void getProducerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    properties.setProperty("acks", "-1");
    properties.setProperty("buffer.memory", "33554432");
    properties.setProperty("retries", "3");
    properties.setProperty("batch.size", "16384");
    properties.setProperty("linger.ms", "20");
    properties.setProperty("max.block.ms", "120000");
    properties.setProperty("request.timeout.ms", "120000");
    properties.setProperty("compression.type", "snappy");
    properties.setProperty("partitioner.class", "com.ebay.traffic.chocolate.flink.nrt.kafka.RoundRobinPartitioner");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.producer", "marketing-tracking-producer");
    properties.setProperty("rheos.topic.schema", "marketing.tracking.behavior.schema");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    assertEquals(properties, unifiedTrackingBotTransformApp.getProducerProperties());
  }

  private RheosEvent createRheosEvent() {
    Map<String, Object> config = new HashMap<>();
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");
    SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
            new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
    int schemaId = serializerHelper.getSchemaId("behavior.pulsar.sojevent.schema");
    Schema schema = serializerHelper.getSchema(schemaId);

    long currentTimeMillis = System.currentTimeMillis();
    RheosEvent rheosEvent = new RheosEvent(schema);
    rheosEvent.setSchemaId(schemaId);
    rheosEvent.setEventCreateTimestamp(currentTimeMillis);
    rheosEvent.setEventSentTimestamp(currentTimeMillis);
    rheosEvent.setProducerId("");
    rheosEvent.setProducerId("");
    rheosEvent.put("guid", new Utf8(""));
    rheosEvent.put("eventTimestamp", currentTimeMillis);
    Map<Utf8, Utf8> map = new HashMap<>();
    rheosEvent.put("applicationPayload", map);
    rheosEvent.put("clientData", map);
    return rheosEvent;
  }

  private byte[] serializeRheosEvent(GenericRecord data) throws IOException {
    EncoderFactory encoderFactory = EncoderFactory.get();
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
      DatumWriter<GenericRecord> writer = getWriter(data);
      writer.write(data, encoder);
      encoder.flush();
      return out.toByteArray();
    }
  }

  public byte[] serializeKey(Long data) {
    return new byte[] {
            (byte) (data >>> 56),
            (byte) (data >>> 48),
            (byte) (data >>> 40),
            (byte) (data >>> 32),
            (byte) (data >>> 24),
            (byte) (data >>> 16),
            (byte) (data >>> 8),
            data.byteValue()
    };
  }

  private DatumWriter<GenericRecord> getWriter(GenericRecord rheosEvent) {
    return new GenericDatumWriter<>(rheosEvent.getSchema());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void transform() throws Exception {
    UnifiedTrackingBotTransformApp.TransformFlatMapFunction transformFunction = new UnifiedTrackingBotTransformApp.TransformFlatMapFunction();
    OneInputStreamOperatorTestHarness<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(transformFunction));
    testHarness.open();

    long currentTimeMillis = System.currentTimeMillis();

    RheosEvent rheosEvent1 = createRheosEvent();

    RheosEvent rheosEvent2 = createRheosEvent();
    rheosEvent2.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
    rheosEvent2.put("pageId", 3084);
    Map<Utf8, Utf8> map2 = new HashMap<>();
    map2.put(new Utf8("chnl"), new Utf8("1"));
    rheosEvent2.put("applicationPayload", map2);
    rheosEvent2.put("clientData", map2);

    RheosEvent rheosEvent3 = createRheosEvent();
    rheosEvent3.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
    rheosEvent3.put("pageId", 3084);
    Map<Utf8, Utf8> map3 = new HashMap<>();
    map3.put(new Utf8("chnl"), new Utf8("7"));
    rheosEvent3.put("applicationPayload", map3);
    rheosEvent3.put("clientData", map3);

    RheosEvent rheosEvent4 = createRheosEvent();
    rheosEvent4.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
    rheosEvent4.put("pageId", 3962);
    Map<Utf8, Utf8> map4 = new HashMap<>();
    map4.put(new Utf8("chnl"), new Utf8("7"));
    rheosEvent4.put("applicationPayload", map4);
    rheosEvent4.put("clientData", map4);

    RheosEvent rheosEvent5 = createRheosEvent();
    rheosEvent5.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
    rheosEvent5.put("pageId", 3962);
    rheosEvent5.put("pageName", "roveropen");
    Map<Utf8, Utf8> map5 = new HashMap<>();
    map5.put(new Utf8("chnl"), new Utf8("7"));
    rheosEvent5.put("applicationPayload", map5);
    rheosEvent5.put("clientData", map5);

    testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 0L, serializeKey(1L), serializeRheosEvent(rheosEvent1)), System.currentTimeMillis());
    testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 0L, serializeKey(2L), serializeRheosEvent(rheosEvent2)), System.currentTimeMillis());
    testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 0L, serializeKey(3L), serializeRheosEvent(rheosEvent3)), System.currentTimeMillis());
    testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 0L, serializeKey(4L), serializeRheosEvent(rheosEvent4)), System.currentTimeMillis());
    testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 0L, serializeKey(5L), serializeRheosEvent(rheosEvent5)), System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(2, output.size());

    List<Long> actual = output.stream().map(record -> ((StreamRecord<Tuple3<String, Long, byte[]>>) record).getValue().f1).collect(Collectors.toList());

    List<Long> expected = new ArrayList<>();
    expected.add(-1L);
    expected.add(-1L);
    assertEquals(expected, actual);
  }

  @Test
  public void parseChannelType() {
    GenericRecord genericRecord = mock(GenericRecord.class);
    when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
      {
      }
    });

    assertNull(UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));

    when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
      {
        put(new Utf8("chnl"), new Utf8("123"));
      }
    });

    assertNull(UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));

    when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
      {
        put(new Utf8("chnl"), new Utf8("7"));
      }
    });

    assertEquals(ChannelIdEnum.SITE_EMAIL, UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));

    when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
      {
        put(new Utf8("chnl"), new Utf8("8"));
      }
    });

    assertEquals(ChannelIdEnum.MRKT_EMAIL, UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));

    when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
      {
      }
    });
    when(genericRecord.get("urlQueryString")).thenReturn(new Utf8(""));

    assertNull(UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));

    when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen/0/e12060/8?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869"));

    assertEquals(ChannelIdEnum.MRKT_EMAIL, UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));

    when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen/0/e12060/7?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869"));

    assertEquals(ChannelIdEnum.SITE_EMAIL, UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));

    when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen"));

    assertNull(UnifiedTrackingBotTransformApp.parseChannelType(genericRecord));
  }
}