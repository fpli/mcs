package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.BehaviorMessageTableV0;
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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BehaviorMessageSinkAppTest {
  private BehaviorMessageSinkApp behaviorMessageSinkApp;
  
  @Before
  public void setUp() throws Exception {
    behaviorMessageSinkApp = new BehaviorMessageSinkApp();
  }

  @Test
  public void getConsumerTopics() {
    assertEquals(Arrays.asList("marketing.tracking.staging.behavior"), behaviorMessageSinkApp.getConsumerTopics());
  }

  @Test
  public void getConsumerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-behavior-message-sink");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("receive.buffer.bytes", "65536");
    properties.setProperty("request.timeout.ms", "305000");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    properties.setProperty("flink.partition-discovery.interval-millis","60000");
    assertEquals(properties, behaviorMessageSinkApp.getConsumerProperties());
  }

  @Test
  public void getSinkBasePath() {
    assertEquals(new Path("temp/unified_tracking/behavior_message"), behaviorMessageSinkApp.getSinkBasePath());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void transformRichMapFunction() throws Exception {
    BehaviorMessageSinkApp.TransformRichMapFunction transformRichMapFunction = new BehaviorMessageSinkApp.TransformRichMapFunction();
    OneInputStreamOperatorTestHarness<ConsumerRecord<byte[], byte[]>, BehaviorMessageTableV0> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(transformRichMapFunction));
    testHarness.open();

    String json = PropertyMgr.getInstance().loadFile("behavior-message.json");

    GenericRecord sourceRecord = createSourceRecord(json);

    byte[] key = serializeKey(0L);
    byte[] value = serializeRheosEvent(new RheosEvent(sourceRecord));

    testHarness.processElement(new ConsumerRecord<>("marketing.tracking.staging.behavior", 0, 0L, key, value), System.currentTimeMillis());

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    assertEquals(1, output.size());
    StreamRecord<BehaviorMessageTableV0> streamRecord = (StreamRecord<BehaviorMessageTableV0>) output.poll();
    assertNotNull(streamRecord);
    BehaviorMessageTableV0 behaviorEvent = streamRecord.getValue();
    assertEquals("6694873385117159424", behaviorEvent.getSnapshotId());
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
    BucketAssigner<BehaviorMessageTableV0, String> sinkBucketAssigner = behaviorMessageSinkApp.getSinkBucketAssigner();
    ZonedDateTime of = ZonedDateTime.of(2020, 8, 4, 7, 30, 0, 0, ZoneId.systemDefault());
    BehaviorMessageTableV0 element = Mockito.mock(BehaviorMessageTableV0.class);
    Mockito.when(element.getEventTimestamp()).thenReturn(of.toInstant().toEpochMilli());
    BucketAssigner.Context context = Mockito.mock(BucketAssigner.Context.class);
    assertEquals("dt=2020-08-04", sinkBucketAssigner.getBucketId(element, context));
    assertEquals(SimpleVersionedStringSerializer.class, sinkBucketAssigner.getSerializer().getClass());
  }

  @Test
  public void getKafkaConsumer() {
    FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> kafkaConsumer = behaviorMessageSinkApp.getKafkaConsumer();
    assertEquals("org.apache.kafka.clients.consumer.ConsumerRecord", kafkaConsumer.getProducedType().getTypeClass().getName());
  }

  @Test
  public void getSinkWriterFactory() {
    BulkWriter.Factory<BehaviorMessageTableV0> sinkWriterFactory = behaviorMessageSinkApp.getSinkWriterFactory();
    assertEquals("org.apache.flink.formats.parquet.ParquetWriterFactory", sinkWriterFactory.getClass().getName());
  }

  @Test
  public void getStreamingFileSink() {
    StreamingFileSink<BehaviorMessageTableV0> streamingFileSink = behaviorMessageSinkApp.getStreamingFileSink();
    assertEquals("org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink", streamingFileSink.getClass().getName());
  }
}