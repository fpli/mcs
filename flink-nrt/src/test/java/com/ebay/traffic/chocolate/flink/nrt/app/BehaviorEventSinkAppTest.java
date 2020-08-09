package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.BehaviorEvent;
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
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.*;

public class BehaviorEventSinkAppTest {
  private BehaviorEventSinkApp behaviorEventSinkApp;

  @Before
  public void setUp() throws Exception {
    behaviorEventSinkApp = new BehaviorEventSinkApp();
  }

  @Test
  public void getConsumerTopics() {
    assertEquals(Collections.singletonList("marketing.tracking.staging.behavior"), behaviorEventSinkApp.getConsumerTopics());
  }

  @Test
  public void getConsumerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-behavior-event-sink");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("receive.buffer.bytes", "65536");
    properties.setProperty("request.timeout.ms", "305000");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    assertEquals(properties, behaviorEventSinkApp.getConsumerProperties());
  }

  @Test
  public void getSinkBasePath() {
    assertEquals(new Path("~/Downloads/flink-test/behavior-event-sink"), behaviorEventSinkApp.getSinkBasePath());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void transformRichMapFunction() throws Exception {
    BehaviorEventSinkApp.TransformRichMapFunction transformRichMapFunction = new BehaviorEventSinkApp.TransformRichMapFunction();
    OneInputStreamOperatorTestHarness<ConsumerRecord<byte[], byte[]>, BehaviorEvent> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(transformRichMapFunction));
    testHarness.open();

    String json = PropertyMgr.getInstance().loadFile("email_open.json");

    GenericRecord sourceRecord = createSourceRecord(json);

    byte[] key = serializeKey(0L);
    byte[] value = serializeRheosEvent(new RheosEvent(sourceRecord));

    testHarness.processElement(new ConsumerRecord<>("marketing.tracking.staging.behavior", 0, 0L, key, value), System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(1, output.size());
    StreamRecord<BehaviorEvent> streamRecord = (StreamRecord<BehaviorEvent>) output.poll();
    assertNotNull(streamRecord);
    BehaviorEvent behaviorEvent = streamRecord.getValue();
    assertEquals("", behaviorEvent.getGuid());
    assertEquals("acd12d5b79ac46b7b5eef0d76510cb1e", behaviorEvent.getAdguid());
    assertEquals(Long.valueOf(6694873385117159424L), behaviorEvent.getSessionskey());
    assertEquals(Long.valueOf(6694873385117159424L), behaviorEvent.getSnapshotid());
    assertEquals(Integer.valueOf(1), behaviorEvent.getSeqnum());
    assertEquals(Integer.valueOf(0), behaviorEvent.getSiteid());
    assertEquals(Integer.valueOf(3962), behaviorEvent.getPageid());
    assertEquals("impression", behaviorEvent.getPagename());
    assertNull(behaviorEvent.getRefererhash());
    assertEquals(Long.valueOf(1596182198056L), behaviorEvent.getEventtimestamp());
    assertEquals("/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=0&sojTags=bu=bu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623",
            behaviorEvent.getUrlquerystring());
    assertEquals("Script=marketingtracking&Agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit" +
                    "/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36,GingerClient/0.11.0-RELEASE&" +
                    "Server=mktcollectionsvc.vip.ebay.com&RemoteIP=10.249.75.9&ContentLength=211&TName=impression" +
                    "&AcceptEncoding=gzip, deflate, br&ForwardedFor=10.249.75.9&TMachine=10.249.75.54",
            behaviorEvent.getClientdata());
    assertEquals("ext=56623&Agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 " +
                    "(KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36&" +
                    "bu=43551630917&euid=c527526a795a414cb4ad11bfaba21b5d&emsid=e11051.m44.l1139" +
                    "&Payload=/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=0&sojTags=bu=bu&bu=43551630917" +
                    "&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623" +
                    "&emid=43551630917",
            behaviorEvent.getApplicationpayload());
    assertEquals("mktcollectionsvc.vip.ebay.com", behaviorEvent.getWebserver());
    assertNull(behaviorEvent.getReferrer());
    assertEquals("43551630917", behaviorEvent.getUserid());
    assertEquals(Integer.valueOf(0), behaviorEvent.getRdt());
    assertEquals("SITE_EMAIL", behaviorEvent.getChanneltype());
    assertEquals("EMAIL_OEPN", behaviorEvent.getChannelaction());
    assertEquals("", behaviorEvent.getDispatchid());
    assertEquals(1, behaviorEvent.getData().size());
    assertTrue(behaviorEvent.getData().iterator().next().isEmpty());
  }

  private GenericRecord createSourceRecord(String json) throws IOException {
    Map<String, Object> map = new HashMap<>();
    map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");

    SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
            new SchemaRegistryAwareAvroSerializerHelper<>(map, GenericRecord.class);

    Schema schema = serializerHelper.getSchema("marketing.tracking.behavior.schema");

    DecoderFactory decoderFactory = new DecoderFactory();
    Decoder decoder = decoderFactory.jsonDecoder(schema, json);
    DatumReader<GenericData.Record> reader =
            new GenericDatumReader<>(schema);
    return reader.read(null, decoder);
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

  @Test
  public void getSinkBucketAssigner() {
    BucketAssigner<BehaviorEvent, String> sinkBucketAssigner = behaviorEventSinkApp.getSinkBucketAssigner();
    ZonedDateTime of = ZonedDateTime.of(2020, 8, 4, 7, 30, 0, 0, ZoneId.systemDefault());
    BehaviorEvent element = Mockito.mock(BehaviorEvent.class);
    Mockito.when(element.getEventtimestamp()).thenReturn(of.toInstant().toEpochMilli());
    BucketAssigner.Context context = Mockito.mock(BucketAssigner.Context.class);
    assertEquals("dt=2020-08-04", sinkBucketAssigner.getBucketId(element, context));
  }
}