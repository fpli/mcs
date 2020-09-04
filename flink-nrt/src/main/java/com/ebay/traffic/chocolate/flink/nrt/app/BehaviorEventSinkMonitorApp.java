package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.BehaviorEvent;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.transformer.BehaviorEventTransformer;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class BehaviorEventSinkMonitorApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, BehaviorEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BehaviorEventSinkMonitorApp.class);

    public static void main(String[] args) throws Exception {
        BehaviorEventSinkMonitorApp sinkApp = new BehaviorEventSinkMonitorApp();
        sinkApp.run();
    }

    @Override
    protected List<String> getConsumerTopics() {
        return  Arrays.asList(PropertyMgr.getInstance()
                .loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
    }

    @Override
    protected Properties getConsumerProperties() {
        return PropertyMgr.getInstance().loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_RHEOS_CONSUMER_MONITOR_PROPERTIES);
    }

    @Override
    protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
        return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
    }

    @Override
    protected Path getSinkBasePath() {
        Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_HDFS_MONITOR_PROPERTIES);
        return new Path(properties.getProperty(PropertyConstants.PATH));
    }

    @Override
    protected StreamingFileSink<BehaviorEvent> getStreamingFileSink() {
        return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
                .build();
    }

    @Override
    protected BulkWriter.Factory<BehaviorEvent> getSinkWriterFactory() {
        return ParquetAvroWriters.forSpecificRecord(BehaviorEvent.class);
    }

    @Override
    protected DataStream<BehaviorEvent> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
        return dataStreamSource.map(new TransformRichMapFunction());
    }

    protected static class TransformRichMapFunction extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, BehaviorEvent> {
        private transient GenericRecordDomainDataDecoder decoder;
        private transient RheosEventDeserializer deserializer;
        private static final String SITE_EMAIL = "SITE_EMAIL";
        private static final String MRKT_EMAIL = "MRKT_EMAIL";

        /*add monitoring metrics

         */
        private static final String RECORDS_INCOMING_COUNTER = "EmailOpenIncoming";
        private static final String RECORDS_LATENCY_COUNTER = "EmailOpenLatency";
        private static final String RECORDS_SITE_EMAIL_COUNTER = "SiteEmailOpenIncoming";
        private static final String RECORDS_MARKETING_EMAIL_COUNTER = "MarketingEmailOpenIncoming";
        private static final String RECORDS_OTHERS_COUNTER = "OtherEmailOpenIncoming";

        private transient Counter recordIncomingCounter;
        private transient Counter latencyCounter;
        private transient Counter siteEmailOpenIncomingCounter;
        private transient Counter marketingEmailOpenIncomingCounter;
        private transient Counter otherEmailOpenIncomingCounter;



        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            deserializer = new RheosEventDeserializer();
            Map<String, Object> config = new HashMap<>();
            Properties properties = PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.BEHAVIOR_EVENT_SINK_APP_RHEOS_CONSUMER_MONITOR_PROPERTIES);
            String rheosServiceUrl = properties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
            config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
            decoder = new GenericRecordDomainDataDecoder(config);

            //register metrics counter
            recordIncomingCounter = getRuntimeContext().getMetricGroup().counter(RECORDS_INCOMING_COUNTER);
            latencyCounter = getRuntimeContext().getMetricGroup().counter(RECORDS_LATENCY_COUNTER);
            siteEmailOpenIncomingCounter = getRuntimeContext().getMetricGroup().counter(RECORDS_SITE_EMAIL_COUNTER);
            marketingEmailOpenIncomingCounter = getRuntimeContext().getMetricGroup().counter(RECORDS_MARKETING_EMAIL_COUNTER);
            otherEmailOpenIncomingCounter = getRuntimeContext().getMetricGroup().counter(RECORDS_OTHERS_COUNTER);
        }

        @Override
        public BehaviorEvent map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
            //increase the incoming record metrics
            recordIncomingCounter.inc();
            long currentTimeMillis = System.currentTimeMillis();
            RheosEvent rheosEvent = deserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
            GenericRecord sourceRecord = decoder.decode(rheosEvent);
            BehaviorEventTransformer behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
            BehaviorEvent behaviorEvent = new BehaviorEvent();
            behaviorEventTransformer.transform(behaviorEvent);
            //increase latency metrics
            if(behaviorEvent.getEventtimestamp() != null){
                latencyCounter.inc(currentTimeMillis - behaviorEvent.getEventtimestamp());
            }
            if(behaviorEvent.getChanneltype() != null){
                switch (behaviorEvent.getChanneltype().toString()) {
                    case SITE_EMAIL:
                        siteEmailOpenIncomingCounter.inc();
                        break;
                    case MRKT_EMAIL:
                        marketingEmailOpenIncomingCounter.inc();
                        break;
                    default:
                        otherEmailOpenIncomingCounter.inc();
                        break;
                }
            }

            return behaviorEvent;
        }
    }

    @Override
    protected BucketAssigner<BehaviorEvent, String> getSinkBucketAssigner() {
        return new CustomEventDateTimeBucketAssigner();
    }

    private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<BehaviorEvent, String> {
        public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());

        @Override
        public String getBucketId(BehaviorEvent element, Context context) {
            return StringConstants.BUCKET_PREFIX + EVENT_DT_FORMATTER.format(Instant.ofEpochMilli(element.getEventtimestamp()));
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
