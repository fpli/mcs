package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.ImkTrckngEventWideMessage;
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
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ImkTrckngEventSinkAppTest {
  private ImkTrckngEventSinkApp imkTrckngEventSinkApp;

  @Before
  public void setUp() throws Exception {
    imkTrckngEventSinkApp = new ImkTrckngEventSinkApp();
  }

  @Test
  public void getConsumerTopics() {
    assertEquals(Collections.singletonList("marketing.tracking.ssl.imk-trckng-event-wide"), imkTrckngEventSinkApp.getConsumerTopics());
  }

  @Test
  public void getConsumerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-vsrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-imk-rvr-trckng-event-sink");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "latest");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    properties.setProperty("ssl.endpoint.identification.algorithm", "");
    assertEquals(properties, imkTrckngEventSinkApp.getConsumerProperties());
  }

  @Test
  public void getSinkBasePath() {
    assertEquals(new Path("temp/imk_tracking/imk_trckng_event"), imkTrckngEventSinkApp.getSinkBasePath());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void transformRichMapFunction() throws Exception {
    ImkTrckngEventSinkApp.TransformRichMapFunction transformRichMapFunction = new ImkTrckngEventSinkApp.TransformRichMapFunction();
    OneInputStreamOperatorTestHarness<ConsumerRecord<byte[], byte[]>, ImkTrckngEventWideMessage> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(transformRichMapFunction));
    testHarness.open();

    String json = PropertyMgr.getInstance().loadFile("imk-trckng-event-wide-message.json");

    GenericRecord sourceRecord = createSourceRecord(json);

    byte[] key = serializeKey(0L);
    byte[] value = serializeRheosEvent(new RheosEvent(sourceRecord));

    testHarness.processElement(new ConsumerRecord<>("marketing.tracking.ssl.imk-trckng-event-wide", 0, 0L, key, value), System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(1, output.size());
    StreamRecord<ImkTrckngEventWideMessage> streamRecord = (StreamRecord<ImkTrckngEventWideMessage>) output.poll();
    assertNotNull(streamRecord);
    ImkTrckngEventWideMessage behaviorEvent = streamRecord.getValue();
    assertEquals("221650", behaviorEvent.getBatchId());
  }

  private GenericRecord createSourceRecord(String json) throws IOException {
    Map<String, Object> map = new HashMap<>();
    map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");

    SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
            new SchemaRegistryAwareAvroSerializerHelper<>(map, GenericRecord.class);

    Schema schema = serializerHelper.getSchema("ImkTrckngEventWideMessage");

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
    BucketAssigner<ImkTrckngEventWideMessage, String> sinkBucketAssigner = imkTrckngEventSinkApp.getSinkBucketAssigner();
    ImkTrckngEventWideMessage element = Mockito.mock(ImkTrckngEventWideMessage.class);
    Mockito.when(element.getEventDt()).thenReturn("2020-08-04");
    BucketAssigner.Context context = Mockito.mock(BucketAssigner.Context.class);
    assertEquals("dt=2020-08-04", sinkBucketAssigner.getBucketId(element, context));
    assertEquals(SimpleVersionedStringSerializer.class, sinkBucketAssigner.getSerializer().getClass());
  }

  @Test
  public void getKafkaConsumer() {
    FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> kafkaConsumer = imkTrckngEventSinkApp.getKafkaConsumer();
    assertEquals("org.apache.kafka.clients.consumer.ConsumerRecord", kafkaConsumer.getProducedType().getTypeClass().getName());
  }

  @Test
  public void getSinkWriterFactory() {
    BulkWriter.Factory<ImkTrckngEventWideMessage> sinkWriterFactory = imkTrckngEventSinkApp.getSinkWriterFactory();
    assertEquals("org.apache.flink.formats.parquet.ParquetWriterFactory", sinkWriterFactory.getClass().getName());
  }

  @Test
  public void getStreamingFileSink() {
    StreamingFileSink<ImkTrckngEventWideMessage> streamingFileSink = imkTrckngEventSinkApp.getStreamingFileSink();
    assertEquals("org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink", streamingFileSink.getClass().getName());
  }
}