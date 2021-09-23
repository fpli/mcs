package com.ebay.app.raptor.chocolate.eventlistener;

import chocolate.EventListenerServiceTest;
import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.RheosConsumerWrapper;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.kafka.*;
import com.ebay.traffic.monitoring.ESMetrics;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.avro.util.Utf8;
import io.ebay.rheos.schema.event.RheosEvent;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class RoverRheosTopicFilterTaskTest {
    private RoverRheosTopicFilterTask roverRheosTopicFilterTask;

    @BeforeClass
    public static void initBeforeTest() {
        ESMetrics.init("chocolate-metrics-", "http://10.148.181.34:9200");
    }

    @Before
    public void setUp() throws Exception {
        RuntimeContext.setConfigRoot(EventListenerServiceTest.class.getClassLoader().getResource
                ("META-INF/configuration/Dev/"));

        RoverRheosTopicFilterTask.init(1l);
        roverRheosTopicFilterTask = RoverRheosTopicFilterTask.getInstance();

        Properties rheosConsumerProperties = getProperties("event-listener-rheos-consumer.properties");

        ApplicationOptions.init();
        RheosConsumerWrapper.init(rheosConsumerProperties);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        KafkaTestHelper.shutdown();

        RoverRheosTopicFilterTask.terminate();
        RheosConsumerWrapper.terminate();
    }

    public RheosEvent getRecord(String pageId, String pageName) {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");
        SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper = new
                SchemaRegistryAwareAvroSerializerHelper<>(
                config, GenericRecord.class);
        Schema schema = serializerHelper.getSchema("behavior.pulsar.sojevent.schema");
        RheosEvent rheosEvent = new RheosEvent(schema, true);
        rheosEvent.setSchemaId(3123);
        rheosEvent.put("guid", UUID.randomUUID());
        rheosEvent.put("eventTimestamp", System.currentTimeMillis());
        rheosEvent.put("cguid", "021efd001561ad9a815f616001da5e55");
        rheosEvent.put("pageId", pageId);
        rheosEvent.put("pageName", pageName);
        rheosEvent.put("userId", "userId");
        rheosEvent.put("siteId", "1,0");
        rheosEvent.put("ciid", "ciid");
        rheosEvent.put("iframe", true);

        Map<String, String> map = new HashMap<String, String>();
        map.put("ForwardFor", "ForwardFor");
        map.put("agent", "agent");
        map.put("remoteIP", "remoteIP");
        map.put("urlQueryString", "urlQueryString");

        rheosEvent.put("clientData", map);
        return rheosEvent;
    }

    public Properties getProperties(String fileName) throws Exception {
        File resourcesDirectory = new File("src/test/resources/META-INF/configuration/Dev/config");
        String resourcePath = resourcesDirectory.getAbsolutePath() + "/";
        String propertiesFile = resourcePath + fileName;
        Properties properties = new Properties();
        properties.load(new FileReader(propertiesFile));
        return properties;
    }


    @Test
    public void testProcessRecords() throws Exception {
        MockProducer<Long, ListenerMessage> producer = new MockProducer<>(true, new LongSerializer(), new ListenerMessageSerializer());
        RoverRheosTopicFilterTask.init(1l);
        RoverRheosTopicFilterTask roverRheosTopicFilterTask = RoverRheosTopicFilterTask.getInstance();

        Properties rheosConsumerProperties = getProperties("event-listener-rheos-consumer.properties");

        ApplicationOptions.init();
        RheosConsumerWrapper.init(rheosConsumerProperties);
        MockConsumer<byte[], RheosEvent> consumerMcs = new MockConsumer<byte[], RheosEvent>(OffsetResetStrategy.EARLIEST);
        Map<String, Object> config = new HashMap<>();
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");
        RheosConsumerWrapper rheosConsumerWrapper = mock(RheosConsumerWrapper.class);
        when(rheosConsumerWrapper.getConsumer()).thenReturn(consumerMcs);
        GenericRecordDomainDataDecoder decoder = mock(GenericRecordDomainDataDecoder.class);
        when(rheosConsumerWrapper.getDecoder()).thenReturn(decoder);
        consumerMcs.assign(Arrays.asList(new TopicPartition("test_topic", 0)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test_topic", 0), 0L);
        consumerMcs.updateBeginningOffsets(beginningOffsets);

        //roi events
        RheosEvent rheosEvent2 = getRecord("3086", "roverroi");
        GenericRecord genericRecord2 = mock(GenericRecord.class);
        when(decoder.decode(rheosEvent2)).thenReturn(genericRecord2);
        HashMap<Utf8, Utf8> data3 = new HashMap<Utf8, Utf8>();
        data3.put(new Utf8("urlQueryString"), new Utf8("/roverroi/1/711-518-1801-29?mpuid=&siteId=&BO=&tranType="));
        data3.put(new Utf8("ForwardFor"), new Utf8("10.148.223.138"));
        data3.put(new Utf8("referrer"), new Utf8("https://www.qa.ebay.com/itm/180010255913"));

        HashMap<Utf8, Utf8> data4 = new HashMap<Utf8, Utf8>();
        data4.put(new Utf8("rvrid"), new Utf8("123456"));
        data4.put(new Utf8("timestamp"), new Utf8("1573463024411"));

        when(genericRecord2.get("clientData")).thenReturn(data3);
        when(genericRecord2.get("applicationPayload")).thenReturn(data4);
        when(genericRecord2.get("eventTimestamp")).thenReturn(12345678L);
        when(genericRecord2.get("pageId")).thenReturn(3086);
        when(genericRecord2.get("guid")).thenReturn("59b405c216e0a4e287dc0e85ffff607c");

        //epn missing click
        RheosEvent rheosEvent3 = getRecord("3084", "rover");
        GenericRecord genericRecord3 = mock(GenericRecord.class);
        when(decoder.decode(rheosEvent3)).thenReturn(genericRecord3);
        HashMap<Utf8, Utf8> data5 = new HashMap<Utf8, Utf8>();
        data5.put(new Utf8("urlQueryString"), new Utf8("/rover/1/711-245192-32613-134/1?ff8=&ff10=117836&ff9=&cmpgnid=-1&ff20=50&tag=displayLauraLisTestRoverCmd-10&ir_DAP_A1=0&ipn=admain2&ff5=&ff6=&ff7=&ff19=&event=adclick&mpt=74612&mpcr=117836&rvr_id=1661323724734&raptor=1&siteid=0,0&ext_id=&mpre=https%3A%2F%2Fwww.ebay.com%2Fb%2FParts-for-Ford-f-150%2F6030%2Fbn_24119272&ff14=unknown&ff13=1&rvrhostname="));
        data5.put(new Utf8("ForwardFor"), new Utf8("10.222.16.20"));

        HashMap<Utf8, Utf8> data6 = new HashMap<Utf8, Utf8>();
        data6.put(new Utf8("rvrid"), new Utf8("1234567"));
        data6.put(new Utf8("timestamp"), new Utf8("1573461248484"));
        data6.put(new Utf8("url_mpre"), new Utf8("http%3A%2F%2Fwww.ebay.de%2Fitm%2Flike%2F113936797595"));

        when(genericRecord3.get("clientData")).thenReturn(data5);
        when(genericRecord3.get("applicationPayload")).thenReturn(data6);
        when(genericRecord3.get("eventTimestamp")).thenReturn(123456789L);
        when(genericRecord3.get("pageId")).thenReturn(3084);
        when(genericRecord3.get("guid")).thenReturn("59b405c216e0a4e287dc0e85ffff607f");

        consumerMcs.addRecord(new ConsumerRecord<byte[], RheosEvent>("test_topic", 0, 1L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) rheosEvent2.get("eventTimestamp")).array(), rheosEvent2));

        consumerMcs.addRecord(new ConsumerRecord<byte[], RheosEvent>("test_topic", 0, 2L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) rheosEvent3.get("eventTimestamp")).array(), rheosEvent3));


        //verify events
        roverRheosTopicFilterTask.processRecords(rheosConsumerWrapper, producer);
        Thread.sleep(5000);
        List<ProducerRecord<Long, ListenerMessage>> history = producer.history();
        assertEquals(2, history.size());
    }

    @Test
    public void parseChannelTypeFromUrlQueryString() {
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("")));
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/10?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertEquals(ChannelIdEnum.MRKT_EMAIL, roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/8?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertEquals(ChannelIdEnum.SITE_EMAIL, roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/7?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
    }

    @Test
    public void parseChannelType() {
        GenericRecord genericRecord = mock(GenericRecord.class);
        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
            }
        });

        assertNull(roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
                put(new Utf8("chnl"), new Utf8("123"));
            }
        });

        assertNull(roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
                put(new Utf8("chnl"), new Utf8("7"));
            }
        });

        assertEquals(ChannelIdEnum.SITE_EMAIL, roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
                put(new Utf8("chnl"), new Utf8("8"));
            }
        });

        assertEquals(ChannelIdEnum.MRKT_EMAIL, roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
            }
        });
        when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen/0/e12060/8?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869"));

        assertEquals(ChannelIdEnum.MRKT_EMAIL, roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen/0/e12060/7?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869"));

        assertEquals(ChannelIdEnum.SITE_EMAIL, roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen"));

        assertNull(roverRheosTopicFilterTask.parseChannelType(genericRecord));
    }
}
