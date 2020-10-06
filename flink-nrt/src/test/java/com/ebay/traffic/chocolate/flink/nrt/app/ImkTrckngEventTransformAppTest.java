package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV5;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ImkTrckngEventTransformAppTest {
  private ImkTrckngEventTransformApp imkTrckngEventTransformApp;

  @Before
  public void setUp() throws Exception {
    imkTrckngEventTransformApp = new ImkTrckngEventTransformApp();
  }

  @Test
  public void getConsumerTopics() {
    assertEquals(Arrays.asList("marketing.tracking.ssl.filtered-paid-search", "marketing.tracking.ssl.filtered-roi",
            "marketing.tracking.ssl.filtered-social-media", "marketing.tracking.ssl.filtered-display"),
            imkTrckngEventTransformApp.getConsumerTopics());
  }

  @Test
  public void getConsumerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-vsrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-imk-trckng-event-transform");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "latest");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    properties.setProperty("ssl.endpoint.identification.algorithm", "");
    assertEquals(properties, imkTrckngEventTransformApp.getConsumerProperties());
  }

  @Test
  public void getProducerTopic() {
    assertEquals("", imkTrckngEventTransformApp.getProducerTopic());
  }

  @Test
  public void getKafkaConsumer() {
    FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> kafkaConsumer = imkTrckngEventTransformApp.getKafkaConsumer();
    assertEquals("org.apache.kafka.clients.consumer.ConsumerRecord", kafkaConsumer.getProducedType().getTypeClass().getName());
  }

  @Test
  public void getKafkaProducer() {
    FlinkKafkaProducer<Tuple3<String, Long, byte[]>> kafkaProducer = imkTrckngEventTransformApp.getKafkaProducer();
    assertEquals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer", kafkaProducer.getClass().getName());
  }

  @Test
  public void getProducerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-vsrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092");
    properties.setProperty("acks", "-1");
    properties.setProperty("buffer.memory", "33554432");
    properties.setProperty("retries", "3");
    properties.setProperty("batch.size", "16384");
    properties.setProperty("linger.ms", "20");
    properties.setProperty("max.block.ms", "120000");
    properties.setProperty("request.timeout.ms", "120000");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("ssl.endpoint.identification.algorithm", "");
    assertEquals(properties, imkTrckngEventTransformApp.getProducerProperties());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void filterEbaySites() throws Exception {
    StreamFilter<FilterMessageV5> operator = new StreamFilter<>(new ImkTrckngEventTransformApp.FilterEbaySites());
    OneInputStreamOperatorTestHarness<FilterMessageV5, FilterMessageV5> testHarness = new OneInputStreamOperatorTestHarness<>(operator);
    testHarness.open();

    String json = PropertyMgr.getInstance().loadFile("filter-message.json");

    FilterMessageV5 filterMessage = createSourceRecord(json);

    FilterMessageV5 testMessage1 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage1.setSnapshotId(1L);
    testMessage1.setChannelType(ChannelType.ROI);
    testMessage1.setChannelAction(ChannelAction.ROI);
    testMessage1.setUri("https://www.qa.ebay.com/i/180010255913");
    testMessage1.setReferer("http://www.ebay.com");

    FilterMessageV5 testMessage2 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage2.setSnapshotId(2L);
    testMessage2.setChannelType(ChannelType.DISPLAY);
    testMessage2.setChannelAction(ChannelAction.SERVE);
    testMessage2.setUri("https://www.qa.ebay.com/itm/180010255913");
    testMessage2.setReferer("http://www.ebay.com");

    FilterMessageV5 testMessage3 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage3.setSnapshotId(3L);
    testMessage3.setChannelType(ChannelType.DISPLAY);
    testMessage3.setChannelAction(ChannelAction.CLICK);
    testMessage3.setUri("https://www.qa.ebay.com/p/180010255913");
    testMessage3.setReferer("www.ebay.com");

    FilterMessageV5 testMessage4 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage4.setSnapshotId(4L);
    testMessage4.setChannelType(ChannelType.DISPLAY);
    testMessage4.setChannelAction(ChannelAction.CLICK);
    testMessage4.setUri("https://www.qa.ebay.com/b/180010255913");
    testMessage4.setReferer("www.ebay.com/ulk/sch/?_nkw=iphone+cases&mkevt=1&mkrid=123&mkcid=2&keyword=testkeyword&crlp=123&MT_ID=1geo_id=123&rlsatarget=123&adpos=1&device=m&loc=1&poi=1&abcId=1&cmpgn=123&sitelnk=123&test=XiangMobile");

    FilterMessageV5 testMessage5 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage5.setSnapshotId(5L);
    testMessage5.setChannelType(ChannelType.DISPLAY);
    testMessage5.setChannelAction(ChannelAction.CLICK);
    testMessage5.setUri("https://www.qa.ebay.com/e/180010255913");
    testMessage5.setReferer("www.ebay.com/ulk/sch/?_nkw=iphone+cases&mkevt=1&mkrid=123&mkcid=2&keyword=testkeyword&crlp=123&MT_ID=1geo_id=123&rlsatarget=123&adpos=1&device=m&loc=1&poi=1&abcId=1&cmpgn=123&sitelnk=123&test=XiangMobile");

    FilterMessageV5 testMessage6 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage6.setSnapshotId(6L);
    testMessage6.setChannelType(ChannelType.DISPLAY);
    testMessage6.setChannelAction(ChannelAction.CLICK);
    testMessage6.setUri("https://www.qa.ebay.com/sch/180010255913");
    testMessage6.setReferer("www.ebay.com/ulk/sch/?_nkw=iphone+cases&mkevt=1&mkrid=123&mkcid=2&keyword=testkeyword&crlp=123&MT_ID=1geo_id=123&rlsatarget=123&adpos=1&device=m&loc=1&poi=1&abcId=1&cmpgn=123&sitelnk=123&test=XiangMobile");

    FilterMessageV5 testMessage7 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage7.setSnapshotId(7L);
    testMessage7.setChannelType(ChannelType.DISPLAY);
    testMessage7.setChannelAction(ChannelAction.CLICK);
    testMessage7.setUri("https://www.qa.ebay.com/deals/180010255913");
    testMessage7.setReferer("www.ebay.com/ulk/sch/?_nkw=iphone+cases&mkevt=1&mkrid=123&mkcid=2&keyword=testkeyword&crlp=123&MT_ID=1geo_id=123&rlsatarget=123&adpos=1&device=m&loc=1&poi=1&abcId=1&cmpgn=123&sitelnk=123&test=XiangMobile");

    FilterMessageV5 testMessage8 = FilterMessageV5.newBuilder(filterMessage).build();
    testMessage8.setSnapshotId(8L);
    testMessage8.setChannelType(ChannelType.DISPLAY);
    testMessage8.setChannelAction(ChannelAction.CLICK);
    testMessage8.setUri("https://www.qa.ebay.com");
    testMessage8.setReferer("www.ebay.com/ulk/sch/?_nkw=iphone+cases&mkevt=1&mkrid=123&mkcid=2&keyword=testkeyword&crlp=123&MT_ID=1geo_id=123&rlsatarget=123&adpos=1&device=m&loc=1&poi=1&abcId=1&cmpgn=123&sitelnk=123&test=XiangMobile");

    testHarness.processElement(testMessage1, System.currentTimeMillis());
    testHarness.processElement(testMessage2, System.currentTimeMillis());
    testHarness.processElement(testMessage3, System.currentTimeMillis());
    testHarness.processElement(testMessage4, System.currentTimeMillis());
    testHarness.processElement(testMessage5, System.currentTimeMillis());
    testHarness.processElement(testMessage6, System.currentTimeMillis());
    testHarness.processElement(testMessage7, System.currentTimeMillis());
    testHarness.processElement(testMessage8, System.currentTimeMillis());

    List<FilterMessageV5> expected = new ArrayList<>();
    expected.add(testMessage1);
    expected.add(testMessage4);
    expected.add(testMessage5);
    expected.add(testMessage6);
    expected.add(testMessage7);
    expected.add(testMessage8);

    List<FilterMessageV5> actual = new ArrayList<>();
    testHarness.getOutput().forEach(elem -> actual.add(((StreamRecord<FilterMessageV5>) elem).getValue()));

    assertEquals(expected, actual);
  }

  private FilterMessageV5 createSourceRecord(String json) throws IOException {
    return FilterMessage.readFromJSON(json);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void decodeFilterMessageFunction() throws Exception {
    ImkTrckngEventTransformApp.DecodeFilterMessageFunction decodeFilterMessageFunction = new ImkTrckngEventTransformApp.DecodeFilterMessageFunction();
    OneInputStreamOperatorTestHarness<ConsumerRecord<byte[], byte[]>, FilterMessageV5> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(decodeFilterMessageFunction));
    testHarness.open();

    String json = PropertyMgr.getInstance().loadFile("filter-message.json");

    GenericRecord genericRecord = createGenericRecord(json);

    byte[] key = serializeKey(0L);
    byte[] value = serializeRheosEvent(new RheosEvent(genericRecord));

    testHarness.processElement(new ConsumerRecord<>("marketing.tracking.ssl.filtered-display", 0, 0L, key, value), System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(1, output.size());
    StreamRecord<FilterMessageV5> streamRecord = (StreamRecord<FilterMessageV5>) output.poll();
    assertNotNull(streamRecord);
    FilterMessageV5 behaviorEvent = streamRecord.getValue();
    assertEquals(Long.valueOf(2333219813745L), behaviorEvent.getSnapshotId());
  }

  private byte[] serializeRheosEvent(RheosEvent data) throws IOException {
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

  private DatumWriter<GenericRecord> getWriter(RheosEvent rheosEvent) {
    return new GenericDatumWriter<>(rheosEvent.getSchema());
  }

  private GenericRecord createGenericRecord(String json) throws IOException {
    Map<String, Object> map = new HashMap<>();
    map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");

    SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
            new SchemaRegistryAwareAvroSerializerHelper<>(map, GenericRecord.class);

    Schema schema = serializerHelper.getSchema("filter-message");

    DecoderFactory decoderFactory = new DecoderFactory();
    Decoder decoder = decoderFactory.jsonDecoder(schema, json);
    DatumReader<GenericData.Record> reader =
            new GenericDatumReader<>(schema);
    return reader.read(null, decoder);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void transformFunction() throws Exception {
    ImkTrckngEventTransformApp.TransformFunction transformFunction = new ImkTrckngEventTransformApp.TransformFunction();
    OneInputStreamOperatorTestHarness<FilterMessageV5, Tuple3<String, Long, byte[]>> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(transformFunction));
    testHarness.open();

    String json = PropertyMgr.getInstance().loadFile("filter-message.json");

    FilterMessageV5 filterMessage = createSourceRecord(json);

    testHarness.processElement(filterMessage, System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(1, output.size());
    StreamRecord<Tuple3<String, Long, byte[]>> streamRecord = (StreamRecord<Tuple3<String, Long, byte[]>>) output.poll();
    assertNotNull(streamRecord);
    Tuple3<String, Long, byte[]> first = streamRecord.getValue();

    assertEquals("marketing.tracking.ssl.imk-trckng-event-wide", first.f0);
    assertEquals(Long.valueOf(123L), first.f1);
  }
}