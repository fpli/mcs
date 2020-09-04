package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.BehaviorMessageTableV0;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
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


public class BehaviorMessageSinkMonitorApp extends AbstractRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, BehaviorMessageTableV0> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BehaviorMessageSinkMonitorApp.class);

    public static void main(String[] args) throws Exception {
        BehaviorMessageSinkMonitorApp sinkApp = new BehaviorMessageSinkMonitorApp();
        sinkApp.run();
    }

    @Override
    protected List<String> getConsumerTopics() {
        return  Arrays.asList(PropertyMgr.getInstance()
                .loadProperty(PropertyConstants.BEHAVIOR_MESSAGE_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
    }

    @Override
    protected Properties getConsumerProperties() {
        return PropertyMgr.getInstance().loadProperty(PropertyConstants.BEHAVIOR_MESSAGE_SINK_APP_RHEOS_CONSUMER_MONITOR_PROPERTIES);
    }

    @Override
    protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
        return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
    }

    @Override
    protected Path getSinkBasePath() {
        Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.BEHAVIOR_MESSAGE_SINK_APP_HDFS_MONITOR_PROPERTIES);
        return new Path(properties.getProperty(PropertyConstants.PATH));
    }

    @Override
    protected StreamingFileSink<BehaviorMessageTableV0> getStreamingFileSink() {
        return StreamingFileSink.forBulkFormat(getSinkBasePath(), getSinkWriterFactory()).withBucketAssigner(getSinkBucketAssigner())
                .build();
    }

    @Override
    protected BulkWriter.Factory<BehaviorMessageTableV0> getSinkWriterFactory() {
        return ParquetAvroWriters.forSpecificRecord(BehaviorMessageTableV0.class);
    }

    @Override
    protected DataStream<BehaviorMessageTableV0> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
        return dataStreamSource.map(new TransformRichMapFunction());
    }

    protected static class TransformRichMapFunction extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, BehaviorMessageTableV0> {
        //    private transient GenericRecordDomainDataDecoder decoder;
        private transient RheosEventDeserializer deserializer;
        private transient DatumReader<BehaviorMessageTableV0> reader;

        /*add monitoring metrics

        */
        private static final String RECORDS_INCOMING_COUNTER = "BehaviorMessageIncoming";

        private transient Counter behaviorMessageIncomingCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            deserializer = new RheosEventDeserializer();
            Map<String, Object> config = new HashMap<>();
            Properties properties = PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.BEHAVIOR_MESSAGE_SINK_APP_RHEOS_CONSUMER_MONITOR_PROPERTIES);
            String rheosServiceUrl = properties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
            config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
//      decoder = new GenericRecordDomainDataDecoder(config);
            reader = new SpecificDatumReader<>(BehaviorMessageTableV0.getClassSchema());

            //register metrics counter
            behaviorMessageIncomingCounter = getRuntimeContext().getMetricGroup().counter(RECORDS_INCOMING_COUNTER);
        }

        @Override
        public BehaviorMessageTableV0 map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
            behaviorMessageIncomingCounter.inc();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(consumerRecord.value(), null);
            BehaviorMessageTableV0 datum = new BehaviorMessageTableV0();
            BehaviorMessageTableV0 read = reader.read(datum, decoder);
            return read;
        }
    }

    @Override
    protected BucketAssigner<BehaviorMessageTableV0, String> getSinkBucketAssigner() {
        return new CustomEventDateTimeBucketAssigner();
    }

    private static class CustomEventDateTimeBucketAssigner implements BucketAssigner<BehaviorMessageTableV0, String> {
        private static final String EVENT_TIMESTAMP = "eventTimestamp";

        public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());

        @Override
        public String getBucketId(BehaviorMessageTableV0 element, Context context) {
            return StringConstants.BUCKET_PREFIX + EVENT_DT_FORMATTER.format(Instant.ofEpochMilli(element.getEventTimestamp()));
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
