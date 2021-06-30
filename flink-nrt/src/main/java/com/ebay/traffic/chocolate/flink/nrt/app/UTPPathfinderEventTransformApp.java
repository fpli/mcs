package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.utp.common.model.rheos.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.function.SherlockioMetricsCompatibleRichFlatMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.transformer.UTPPathfinderEventTransformer;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.io.StringReader;

/**
 * Receive events from pathfinder and then send to utp topic.
 *
 * @author Zhongdan Fan
 * @since 2021/6/15
 */
public class UTPPathfinderEventTransformApp
        extends AbstractRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {

    private static final long DEFAULT_SNAPSHOT_ID = -1L;

    public static void main(String[] args) throws Exception {
        UTPPathfinderEventTransformApp transformApp = new UTPPathfinderEventTransformApp();
        transformApp.run();
    }

    @Override
    protected void loadProperty() {
        this.env_config = PropertyMgr.getInstance().loadYaml(PropertyConstants.UTP_PATHFINDER_EVENT_TRANSFORM_APP_YAML);
    }

    @Override
    protected List<String> getConsumerTopics() {
        Map<String, Object> source = (Map<String, Object>) env_config.get(PropertyConstants.SOURCE);
        String topics = (String) source.get(PropertyConstants.TOPIC);
        return Arrays.asList(topics.split(StringConstants.COMMA));
    }

    @Override
    protected Properties getConsumerProperties() throws IOException {
        Map<String, Object> source = (Map<String, Object>) env_config.get(PropertyConstants.SOURCE);
        Properties consumerProperties = new Properties();
        consumerProperties.load(new StringReader((String) source.get(PropertyConstants.PRORERTIES)));
        return consumerProperties;
    }

    @Override
    protected String getProducerTopic() {
        return StringConstants.EMPTY;
    }

    @Override
    protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() throws IOException {
        return new FlinkKafkaConsumer<>(getConsumerTopics(),
                new DefaultKafkaDeserializationSchema(), getConsumerProperties());
    }

    @Override
    protected FlinkKafkaProducer<Tuple3<String, Long, byte[]>> getKafkaProducer() throws IOException {
        return new FlinkKafkaProducer<>(getProducerTopic(), new DefaultKafkaSerializationSchema(),
                getProducerProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }

    @Override
    protected Properties getProducerProperties() throws IOException {
        Map<String, Object> transform = (Map<String, Object>) env_config.get(PropertyConstants.TRANSFORM);
        Properties producerProperties = new Properties();
        producerProperties.load(new StringReader((String) transform.get(PropertyConstants.PRORERTIES)));
        return producerProperties;
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
        private transient SherlockioMetrics sherlockioMetrics;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            deserializer = new RheosEventDeserializer();
            Map<String, Object> config = new HashMap<>();
            Map<String, Object> env_config = PropertyMgr.getInstance().loadYaml(PropertyConstants.UTP_PATHFINDER_EVENT_TRANSFORM_APP_YAML);
            Map<String, Object> source = (Map<String, Object>) env_config.get(PropertyConstants.SOURCE);
            Properties consumerProperties = new Properties();
            consumerProperties.load(new StringReader((String) source.get(PropertyConstants.PRORERTIES)));
            String rheosServiceUrl = consumerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
            config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
            decoder = new GenericRecordDomainDataDecoder(config);
            encoderFactory = EncoderFactory.get();
            Map<String, Object> transform = (Map<String, Object>) env_config.get(PropertyConstants.TRANSFORM);
            Properties producerProperties = new Properties();
            producerProperties.load(new StringReader((String) transform.get(PropertyConstants.PRORERTIES)));
            config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
            SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
                    new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
            String schemaName = (String) producerProperties.get(RheosConstants.RHEOS_TOPIC_SCHEMA);
            schemaId = serializerHelper.getSchemaId(schemaName);
            schema = serializerHelper.getSchema(schemaName);
            producer = (String) producerProperties.get(RheosConstants.RHEOS_PRODUCER);
            topic = (String) transform.get(PropertyConstants.TOPIC);
            sherlockioMetrics = SherlockioMetrics.getInstance();
            sherlockioMetrics.setJobName("UTPPathfinderEventTransformApp");
        }

        @SuppressWarnings("unchecked")
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
            String consumerTopic = consumerRecord.topic();
            RheosEvent sourceRheosEvent = deserializer.deserialize(consumerTopic, consumerRecord.value());
            GenericRecord sourceRecord = decoder.decode(sourceRheosEvent);
            UTPPathfinderEventTransformer transformer = new UTPPathfinderEventTransformer(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), sourceRecord, sourceRheosEvent, schemaVersion);
            if (!transformer.isValid()) {
                sherlockioMetrics.meter("UTPPathfinderEventInvalid");
                return;
            }

            UnifiedTrackingRheosMessage message = new UnifiedTrackingRheosMessage();
            transformer.transform(message);
            RheosEvent rheosEvent = getRheosEvent(message);

            sherlockioMetrics.meter("UTPRoverEvent", 1, Field.of("channelType", message.getChannelType()),
                    Field.of("actionType", message.getActionType()), Field.of("isBot", message.getIsBot()));

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
