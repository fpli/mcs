package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UTPPathfinderEventTransformAppTest {
    private UTPPathfinderEventTransformApp utpPathfinderEventTransformApp;
    private SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper;
    private GenericRecordDomainDataDecoder decoder;
    private RheosEventDeserializer deserializer;

    @Before
    public void setUp() throws Exception {
        utpPathfinderEventTransformApp = new UTPPathfinderEventTransformApp();
        Map<String, Object> config = new HashMap<>();
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");
        serializerHelper = new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
        deserializer = new RheosEventDeserializer();
        decoder = new GenericRecordDomainDataDecoder(config);
    }

    @Test
    public void getConsumerTopics() {
        utpPathfinderEventTransformApp.loadProperty();
        assertEquals(Arrays.asList("behavior.pulsar.customized.tracking.install", "behavior.pulsar.customized.tracking.install.bot"), utpPathfinderEventTransformApp.getConsumerTopics());
    }

    @Test
    public void getConsumerProperties() throws IOException {
        utpPathfinderEventTransformApp.loadProperty();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "rhs-nbrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-6.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-7.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-8.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-9.rheos-streaming-qa.svc.32.tess.io:9092,rhs-nbrvkiaa-kfk-lvs-10.rheos-streaming-qa.svc.32.tess.io:9092");
        properties.setProperty("group.id", "marketing-tracking-consumer-utp-install-event-transform");
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("receive.buffer.bytes", "16777216");
        properties.setProperty("request.timeout.ms", "305000");
        properties.setProperty("sasl.mechanism", "IAF");
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required\niafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\"\niafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\"\niafEnv=\"staging\";");
        properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
        properties.setProperty("flink.partition-discovery.interval-millis","60000");
        assertEquals(properties, utpPathfinderEventTransformApp.getConsumerProperties());
    }

    @Test
    public void getProducerTopic() {
        assertEquals("", utpPathfinderEventTransformApp.getProducerTopic());
    }

    @Test
    public void getKafkaConsumer() throws IOException {
        utpPathfinderEventTransformApp.loadProperty();
        FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> kafkaConsumer = utpPathfinderEventTransformApp.getKafkaConsumer();
        assertEquals("org.apache.kafka.clients.consumer.ConsumerRecord", kafkaConsumer.getProducedType().getTypeClass().getName());
    }

    @Test
    public void getKafkaProducer() throws IOException {
        utpPathfinderEventTransformApp.loadProperty();
        FlinkKafkaProducer<Tuple3<String, Long, byte[]>> kafkaProducer = utpPathfinderEventTransformApp.getKafkaProducer();
        assertEquals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer", kafkaProducer.getClass().getName());
    }

    @Test
    public void getProducerProperties() throws IOException {
        utpPathfinderEventTransformApp.loadProperty();
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
        properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required\niafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\"\niafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\"\niafEnv=\"staging\";");
        properties.setProperty("rheos.producer", "marketing-tracking-producer");
        properties.setProperty("rheos.topic.schema", "marketing.tracking.events.schema");
        properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
        assertEquals(properties, utpPathfinderEventTransformApp.getProducerProperties());
    }

    private RheosEvent createRheosEvent() {
        int schemaId = serializerHelper.getSchemaId("behavior.sojourner.sojevent.schema");
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
        UTPPathfinderEventTransformApp.TransformFlatMapFunction transformFunction = new UTPPathfinderEventTransformApp.TransformFlatMapFunction();
        OneInputStreamOperatorTestHarness<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(transformFunction));
        testHarness.open();

        long currentTimeMillis = System.currentTimeMillis();

        RheosEvent rheosEvent1 = createRheosEvent();

        RheosEvent rheosEvent2 = createRheosEvent();
        rheosEvent2.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
        rheosEvent2.put("pageId", 3203626);
        rheosEvent2.put("pageName", "Ginger.v1.batchtrack.POST");
        Map<Utf8, Utf8> map2 = new HashMap<>();
        map2.put(new Utf8("mppid"), new Utf8("1"));
        rheosEvent2.put("applicationPayload", map2);
        rheosEvent2.put("clientData", map2);

        RheosEvent rheosEvent3 = createRheosEvent();
        rheosEvent3.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
        rheosEvent3.put("pageId", 3203764);
        rheosEvent3.put("pageName", "Ginger.v1.batchtrack.POST");
        Map<Utf8, Utf8> map3 = new HashMap<>();
        map3.put(new Utf8("mppid"), new Utf8("777"));
        rheosEvent3.put("applicationPayload", map3);
        rheosEvent3.put("clientData", map3);

        RheosEvent rheosEvent4 = createRheosEvent();
        rheosEvent4.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
        rheosEvent4.put("pageId", 2050535);
        rheosEvent4.put("pageName", "Ginger.v1.batchtrack.POST");
        Map<Utf8, Utf8> map4 = new HashMap<>();
        map4.put(new Utf8("mppid"), new Utf8("7"));
        rheosEvent4.put("applicationPayload", map4);

        RheosEvent rheosEvent5 = createRheosEvent();
        rheosEvent5.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
        rheosEvent5.put("pageId", 3962);
        rheosEvent5.put("pageName", "roveropen");
        Map<Utf8, Utf8> map5 = new HashMap<>();
        map5.put(new Utf8("chnl"), new Utf8("7"));
        rheosEvent5.put("applicationPayload", map5);
        rheosEvent5.put("clientData", map5);

        RheosEvent rheosEvent6 = createRheosEvent();
        rheosEvent6.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
        rheosEvent6.put("pageId", 2050535);
        Map<Utf8, Utf8> map6 = new HashMap<>();
        map6.put(new Utf8("mppid"), new Utf8("7"));
        rheosEvent6.put("applicationPayload", map6);
        rheosEvent6.put("clientData", map6);

        RheosEvent rheosEvent7 = createRheosEvent();
        rheosEvent7.put("urlQueryString", new Utf8(String.format("test-value-%d", currentTimeMillis)));
        rheosEvent7.put("pageId", 2050535);
        rheosEvent7.put("pageName", "wrongpagename");
        Map<Utf8, Utf8> map7 = new HashMap<>();
        map7.put(new Utf8("mppid"), new Utf8("7"));
        rheosEvent7.put("applicationPayload", map7);
        rheosEvent7.put("clientData", map7);

        testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.customized.tracking.install", 0, 0L, serializeKey(1L), serializeRheosEvent(rheosEvent1)), System.currentTimeMillis());
        testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.customized.tracking.install", 0, 0L, serializeKey(2L), serializeRheosEvent(rheosEvent2)), System.currentTimeMillis());
        testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.customized.tracking.install", 0, 0L, serializeKey(3L), serializeRheosEvent(rheosEvent3)), System.currentTimeMillis());
        testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.customized.tracking.install", 0, 0L, serializeKey(4L), serializeRheosEvent(rheosEvent4)), System.currentTimeMillis());
        testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.customized.tracking.install", 0, 0L, serializeKey(5L), serializeRheosEvent(rheosEvent5)), System.currentTimeMillis());
        testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.customized.tracking.install", 0, 0L, serializeKey(6L), serializeRheosEvent(rheosEvent6)), System.currentTimeMillis());
        testHarness.processElement(new ConsumerRecord<>("behavior.pulsar.customized.tracking.install", 0, 0L, serializeKey(7L), serializeRheosEvent(rheosEvent7)), System.currentTimeMillis());


        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(3, output.size());

        StreamRecord<Tuple3<String, Long, byte[]>> first = (StreamRecord<Tuple3<String, Long, byte[]>>) output.poll();
        assertEquals(Long.valueOf(-1L), first.getValue().f1);
        assertEquals("3203626", String.valueOf(deserializeGenericRecord("marketing.tracking.events.schema", first.getValue().f2).get("pageId")));
        assertEquals("1", String.valueOf(deserializeGenericRecord("marketing.tracking.events.schema", first.getValue().f2).get("partner")));

        StreamRecord<Tuple3<String, Long, byte[]>> second = (StreamRecord<Tuple3<String, Long, byte[]>>) output.poll();
        assertEquals(Long.valueOf(-1L), second.getValue().f1);
        assertEquals("3203764", String.valueOf(deserializeGenericRecord("marketing.tracking.events.schema", second.getValue().f2).get("pageId")));
        assertEquals("777", String.valueOf(deserializeGenericRecord("marketing.tracking.events.schema", second.getValue().f2).get("partner")));

        StreamRecord<Tuple3<String, Long, byte[]>> third = (StreamRecord<Tuple3<String, Long, byte[]>>) output.poll();
        assertEquals(Long.valueOf(-1L), third.getValue().f1);
        assertEquals("2050535", String.valueOf(deserializeGenericRecord("marketing.tracking.events.schema", third.getValue().f2).get("pageId")));
        assertEquals("7", String.valueOf(deserializeGenericRecord("marketing.tracking.events.schema", third.getValue().f2).get("partner")));

    }

    private GenericRecord deserializeGenericRecord(String topic, byte[] value) {
        RheosEvent sourceRheosEvent = deserializer.deserialize(topic, value);
        return decoder.decode(sourceRheosEvent);
    }
}
