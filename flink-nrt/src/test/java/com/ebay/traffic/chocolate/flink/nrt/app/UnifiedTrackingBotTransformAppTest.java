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
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.operators.StreamMap;
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

import static org.junit.Assert.*;
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

  @SuppressWarnings("unchecked")
  @Test
  public void filter() throws Exception {
    StreamFilter<GenericRecord> operator = new StreamFilter<>(new UnifiedTrackingBotTransformApp.FilterFunction());
    OneInputStreamOperatorTestHarness<GenericRecord, GenericRecord> testHarness = new OneInputStreamOperatorTestHarness<>(operator);
    testHarness.open();

    GenericRecord genericRecord1 = createRheosEvent();

    GenericRecord genericRecord2 = createRheosEvent();
    genericRecord2.put("pageId", 3084);
    genericRecord2.put("applicationPayload", new HashMap<Utf8, Utf8>() {
      {
        put(new Utf8("chnl"), new Utf8("1"));
      }
    });

    GenericRecord genericRecord3 = createRheosEvent();
    genericRecord3.put("pageId", 3084);
    genericRecord3.put("applicationPayload", new HashMap<Utf8, Utf8>() {
      {
        put(new Utf8("chnl"), new Utf8("7"));
      }
    });

    GenericRecord genericRecord4 = createRheosEvent();
    genericRecord4.put("pageId", 3962);
    genericRecord4.put("applicationPayload", new HashMap<Utf8, Utf8>() {
      {
        put(new Utf8("chnl"), new Utf8("7"));
      }
    });

    GenericRecord genericRecord5 = createRheosEvent();
    genericRecord5.put("pageId", 3962);
    genericRecord5.put("pageName", "roveropen");
    genericRecord5.put("applicationPayload", new HashMap<Utf8, Utf8>() {
      {
        put(new Utf8("chnl"), new Utf8("7"));
      }
    });


    testHarness.processElement(genericRecord1, System.currentTimeMillis());
    testHarness.processElement(genericRecord2, System.currentTimeMillis());
    testHarness.processElement(genericRecord3, System.currentTimeMillis());
    testHarness.processElement(genericRecord4, System.currentTimeMillis());
    testHarness.processElement(genericRecord5, System.currentTimeMillis());

    List<GenericRecord> expected = new ArrayList<>();
    expected.add(genericRecord3);
    expected.add(genericRecord5);


    List<GenericRecord> actual = new ArrayList<>();
    testHarness.getOutput().forEach(elem -> actual.add(((StreamRecord<GenericRecord>) elem).getValue()));
    for (int i = 0; i < actual.size(); i++) {
      GenericRecord genericRecord = actual.get(i);
      for (Schema.Field a : genericRecord.getSchema().getFields()) {
        String fn = a.name();
        Object vaue = genericRecord.get(fn);
        Object o = expected.get(i).get(a.name());
        assertEquals(vaue, o);
      }
    }
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
    return rheosEvent;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void decode() throws Exception {
    UnifiedTrackingBotTransformApp.DecodeFunction decodeFilterMessageFunction = new UnifiedTrackingBotTransformApp.DecodeFunction();
    OneInputStreamOperatorTestHarness<ConsumerRecord<byte[], byte[]>, GenericRecord> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(decodeFilterMessageFunction));
    testHarness.open();

    GenericRecord rheosEvent = createRheosEvent();
    long currentTimeMillis = System.currentTimeMillis();
    rheosEvent.put("urlQueryString", String.format("test-value-%d", currentTimeMillis));
    rheosEvent.put("guid", "");
    rheosEvent.put("eventTimestamp", currentTimeMillis);
    Map<String, String> map = new HashMap<>();
    rheosEvent.put("applicationPayload", map);
    rheosEvent.put("clientData", map);

    byte[] key = serializeKey(0L);
    byte[] value = serializeRheosEvent(rheosEvent);

    testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 0L, key, value), System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(1, output.size());
    StreamRecord<GenericRecord> streamRecord = (StreamRecord<GenericRecord>) output.poll();
    assertNotNull(streamRecord);
    GenericRecord behaviorEvent = streamRecord.getValue();
    assertEquals(new Utf8(String.format("test-value-%d", currentTimeMillis)), behaviorEvent.get("urlQueryString"));
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
    UnifiedTrackingBotTransformApp.TransformFunction transformFunction = new UnifiedTrackingBotTransformApp.TransformFunction();
    OneInputStreamOperatorTestHarness<GenericRecord, Tuple3<String, Long, byte[]>> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(transformFunction));
    testHarness.open();

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
    rheosEvent.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
    rheosEvent.put("pageId", 3962);
    rheosEvent.put("guid", new Utf8(""));
    rheosEvent.put("eventTimestamp", currentTimeMillis);
    Map<Utf8, Utf8> map = new HashMap<>();
    map.put(new Utf8("chnl"), new Utf8("7"));
    rheosEvent.put("applicationPayload", map);
    rheosEvent.put("clientData", map);

    testHarness.processElement(rheosEvent, System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(1, output.size());
    StreamRecord<Tuple3<String, Long, byte[]>> streamRecord = (StreamRecord<Tuple3<String, Long, byte[]>>) output.poll();
    assertNotNull(streamRecord);
    Tuple3<String, Long, byte[]> first = streamRecord.getValue();

    assertEquals("marketing.tracking.staging.behavior", first.f0);
    assertEquals(Long.valueOf(-1L), first.f1);
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