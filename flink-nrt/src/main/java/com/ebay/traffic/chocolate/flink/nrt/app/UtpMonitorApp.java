package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.*;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.provider.monitor.DimensionEntity;
import com.ebay.traffic.chocolate.flink.nrt.util.MetricsUtil;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.utp.common.model.Message;
import com.ebay.traffic.chocolate.utp.common.model.MessageRoot;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Receive performance marketing events from utp topic, aggregate metrics of different kinds of messages to sherlock
 *
 * @author yuhxiao
 * @since 2021/11/10
 */
public class UtpMonitorApp {
    protected StreamExecutionEnvironment streamExecutionEnvironment;

    protected static final long CHECK_POINT_PERIOD = TimeUnit.SECONDS.toMillis(180);

    protected static final long MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(1);

    protected static final long CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(300);

    protected static final int MAX_CONCURRENT_CHECK_POINTS = 1;

    private static final ObjectMapper mapper = new ObjectMapper();

    protected Map<String, Object> config;

    public static void main(String[] arg) throws Exception {
        UtpMonitorApp utpMonitorApp = new UtpMonitorApp();
        utpMonitorApp.run();
    }

    protected void run() throws Exception {
        loadProperty();
        streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        prepareBaseExecutionEnvironment();
        addSource();
        execute();
    }

    private void execute() throws Exception {
        Map<String, Object> job = (Map<String, Object>) config.get("job");
        streamExecutionEnvironment.execute((String) job.get("name"));
    }

    protected void addSource() throws IOException {
        Map<String, Object> source = (Map<String, Object>) config.get("source");
        String name = (String) source.get("name");
        String uid = (String) source.get("uid");
        streamExecutionEnvironment.addSource(getKafkaConsumer()).name(name).uid(uid)
                .map(new Deserialize()).name("deserialize").uid("deserialize")
                .map(new Transform());
    }

    protected void loadProperty() {
        this.config = PropertyMgr.getInstance().loadYaml(PropertyConstants.UTP_MONITOR_APP_YAML);
    }

    protected void prepareBaseExecutionEnvironment() {
        streamExecutionEnvironment.enableCheckpointing(CHECK_POINT_PERIOD);
        streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECK_POINTS);
        streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(CHECK_POINT_TIMEOUT);
        streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECK_POINTS);
        streamExecutionEnvironment.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() throws IOException {
        Map<String, Object> source = (Map<String, Object>) config.get("source");
        String topics = (String) source.get("topic");

        Properties consumerProperties = new Properties();
        consumerProperties.load(new StringReader((String) source.get("properties")));
        return new FlinkKafkaConsumer<>(Arrays.asList(topics.split(",")),
                new DefaultKafkaDeserializationSchema(), consumerProperties);
    }

    private static class Deserialize extends RichMapFunction<ConsumerRecord<byte[], byte[]>, UnifiedTrackingRheosMessage> {
        private transient DatumReader<GenericRecord> rheosHeaderReader;
        private transient DatumReader<UnifiedTrackingRheosMessage> reader;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Schema rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
            rheosHeaderReader = new GenericDatumReader<>(rheosHeaderSchema);
            reader = new SpecificDatumReader<>(UnifiedTrackingRheosMessage.getClassSchema());
        }

        @Override
        public UnifiedTrackingRheosMessage map(ConsumerRecord<byte[], byte[]> value) throws Exception {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value.value(), null);
            rheosHeaderReader.read(null, decoder);
            UnifiedTrackingRheosMessage datum = new UnifiedTrackingRheosMessage();
            datum = reader.read(datum, decoder);
            return datum;
        }
    }

    private static class Transform extends RichMapFunction<UnifiedTrackingRheosMessage, String> {
        private SherlockioMetrics sherlockioMetrics;
        private transient ConcurrentHashMap<String, Counter> counters;

        @Override
        public void open(Configuration parameters) throws Exception {
            counters = new ConcurrentHashMap<>();
            Properties properties = PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Start refreshing the metricsTarget");
                    MetricsUtil.updateCache(properties.getProperty(PropertyConstants.FIDELIUS_URL));
                }
            }, 0, 10000);
            SherlockioMetrics.init(properties.getProperty(PropertyConstants.MONITOR_SHERLOCKIO_NAMESPACE),
                    properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT), properties.getProperty(PropertyConstants.SHERLOCKIO_USER));
            sherlockioMetrics = SherlockioMetrics.getInstance();
            sherlockioMetrics.setJobName(PropertyConstants.UNIFIED_TRACKING_MONITOR_JOBNAME);
        }

        @SafeVarargs
        public final Counter getCounter(RuntimeContext runtimeContext, String metricsName, Field<String, Object>... fields) {
            StringBuilder counterKey = new StringBuilder(metricsName);
            for (Field<String, Object> stringObjectField : fields) {
                counterKey.append(stringObjectField.getValue().toString());
            }
            if (!counters.containsKey(counterKey.toString())) {
                MetricGroup metricGroup = runtimeContext
                        .getMetricGroup();
                for (Field<String, Object> field : fields) {
                    metricGroup = metricGroup.addGroup(field.getKey(), field.getValue().toString());
                }
                Counter counter = metricGroup.counter(metricsName);
                counters.put(counterKey.toString(), counter);
            }
            return counters.get(counterKey.toString());
        }

        @Override
        public String map(UnifiedTrackingRheosMessage message) {
            String channelType = nullVerifier(message.getChannelType());
            String actionType = nullVerifier(message.getActionType());
            String producer = nullVerifier(message.getService());
            String isBot = nullVerifier(message.getIsBot().toString());
            String isUep = nullVerifier(getUEP(message.getPayload()));
            String platform = nullVerifier(getPlatform(message));
            String site = nullVerifier(String.valueOf(message.getSiteId()));

            getCounter(getRuntimeContext(), "unified_tracking_incoming_v2",
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer),
                    Field.of("isBot", isBot),
                    Field.of("isUEP", isUep),
                    Field.of("platform", platform),
                    Field.of("site", site)).inc();

            sherlockioMetrics.meterByGauge("unified_tracking_incoming_v3", 1,
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer),
                    Field.of("isBot", isBot),
                    Field.of("isUEP", isUep),
                    Field.of("platform", platform),
                    Field.of("site", site)
            );

            sherlockioMetrics.meterByGauge("unified_tracking_latency_v3",
                    message.getEventTs() - message.getProducerEventTs(),
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer)
            );

            String url = nullVerifier(message.getUrl());
            String mkcid = getDuplicateValue(url, "mkcid");
            String mkrid = getDuplicateValue(url, "mkrid");
            String mkpid = getDuplicateValue(url, "mkpid");
            String mksid = getDuplicateValue(url, "mksid");

            sherlockioMetrics.meterByGauge("unified_tracking_duplicate_incoming_v3", 1,
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer),
                    Field.of("isBot", isBot),
                    Field.of("mkcid", mkcid),
                    Field.of("mkrid", mkrid),
                    Field.of("mkpid", mkpid),
                    Field.of("mksid", mksid)
            );

            if ("true".equals(isUep.toLowerCase())) {
                try {
                    List<String> messageId = getMessageId(message.getPayload());
                    String cnvId = nullVerifier(getCnvId(message.getPayload()));
                    for (int i = 0; i < messageId.size(); i++) {
                        System.out.println(messageId.get(i));
                        sherlockioMetrics.meterByGauge("unified_tracking_payload_v3", 1,
                                Field.of("channel", channelType),
                                Field.of("action", actionType),
                                Field.of("producer", producer),
                                Field.of("isBot", isBot),
                                Field.of("isUEP", isUep),
                                Field.of("platform", platform),
                                Field.of("messageId", messageId.get(i)),
                                Field.of("cnvId", cnvId),
                                Field.of("is1stMId", i == 0 ? "true" : "false")
                        );
                    }
                } catch (Exception e) {
                    sherlockioMetrics.meterByGauge("unified_tracking_payload_parsing_error", 1,
                            Field.of("channel", channelType),
                            Field.of("action", actionType),
                            Field.of("producer", producer)
                    );
                }
            }
            return "";
        }

        @Override
        public void close() {
            SherlockioMetrics.getInstance().close();
        }
    }

    private static List<String> getMessageId(Map<String, String> payload) throws Exception {
        List<String> nullMessageList = new ArrayList<>();
        nullMessageList.add("null");
        String messageListString = payload.getOrDefault("annotation.mesg.list", "[]");
        List<Message> messageList = mapper.readValue(messageListString, new TypeReference<List<Message>>() {
        });
        if (messageList.size() == 0) {
            return nullMessageList;
        }
        return messageList
                .stream()
                .map(e -> {
                    if (e.mesgId == null) {
                        return "null";
                    }
                    return e.mesgId;
                })
                .collect(Collectors.toList());
    }

    /**
     * Check the payload, see if there is a exist is getCnvId
     *
     * @param payload the payload to be read
     * @return the corresponding value if so, otherwise return "null"
     */
    private static String getCnvId(Map<String, String> payload) {
        return payload.getOrDefault("annotation.cnv.id", "null");
    }

    /**
     * Check if the input is null
     *
     * @param dimension the dimension that needs to be checked
     * @return return itself if it's not null, otherwise return "null"
     */
    private static String nullVerifier(String dimension) {
        if (dimension != null && StringUtils.isNotEmpty(dimension.trim())) {
            return dimension;
        }
        return "null";
    }

    /**
     * Check the payload, see if there is a exist is UEP key
     *
     * @param payload the payload to be read
     * @return the corresponding value if so, otherwise return "null"
     */
    private static String getUEP(Map<String, String> payload) {
        return payload.getOrDefault("isUEP", "null");
    }

    /**
     * Check the message, see what platform generated the message
     *
     * @param message the message to be read
     * @return the String platform
     */
    private static String getPlatform(UnifiedTrackingRheosMessage message) {
        String appId = message.getAppId();
        String userAgent = message.getUserAgent();
        if (appId == null) {
            if (userAgent != null) {
                return "DESKTOP";
            } else {
                return "null";
            }
        } else {
            switch (appId) {
                case "3564":
                    return "MOBILE_PHONE_WEB";
                case "1115":
                    return "MOBILE_TABLET_WEB";
                case "1462":
                    return "IPHONE";
                case "2878":
                    return "IPAD";
                case "2571":
                    return "ANDROID";
                default:
                    return "null";
            }
        }
    }

    public static String getDuplicateValue(String url, String duplicateItemName) {
        try {
            UriComponents uriComponents = UriComponentsBuilder.fromUriString(url).build();
            MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
            if (parameters.containsKey(duplicateItemName)) {
                List<String> items = parameters
                        .get(duplicateItemName)
                        .stream()
                        .map(String::trim)
                        .distinct()
                        .map(e -> {
                            if (e.length() == 0) {
                                return "EMPTY";
                            }
                            return e;
                        })
                        .sorted(StringUtils::compare)
                        .collect(Collectors.toList());

                boolean duplicateOrNonExist = (items.size() > 1) || (items.size() == 1 && "EMPTY".equals(items.get(0)));
                if (duplicateOrNonExist) {
                    return StringUtils.join(items, '+');
                } else {
                    return "DEFAULT";
                }
            } else {
                return "NULL";
            }
        } catch (Exception e) {
            return "EXCEPTION";
        }
    }
}
