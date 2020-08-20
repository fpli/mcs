package com.ebay.app.raptor.chocolate.eventlistener;

import chocolate.EventListenerServiceTest;
import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
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
    private MockProducer<Long, ListenerMessage> producer;
    private MockProducer<Long, BehaviorMessage> behaviorProducer;
    private RoverRheosTopicFilterTask roverRheosTopicFilterTask;
    private RheosConsumerWrapper rheosConsumerWrapper;
    private MockConsumer<byte[], RheosEvent> consumerMcs;
    private GenericRecordDomainDataDecoder decoder;

    @BeforeClass
    public static void initBeforeTest() {
        ESMetrics.init("chocolate-metrics-", "http://10.148.181.34:9200");
    }

    @Before
    public void setUp() throws Exception {
        RuntimeContext.setConfigRoot(EventListenerServiceTest.class.getClassLoader().getResource
                ("META-INF/configuration/Dev/"));

        producer = new MockProducer<>(true, new LongSerializer(), new ListenerMessageSerializer());
        behaviorProducer = new MockProducer<>(true, new LongSerializer(), new BehaviorMessageSerializer());

        RoverRheosTopicFilterTask.init(1l);
        roverRheosTopicFilterTask = RoverRheosTopicFilterTask.getInstance();

        Properties rheosConsumerProperties = getProperties("event-listener-rheos-consumer.properties");

        ApplicationOptions.init();
        RheosConsumerWrapper.init(rheosConsumerProperties);
        consumerMcs = new MockConsumer<byte[], RheosEvent>(OffsetResetStrategy.EARLIEST);
        decoder = mock(GenericRecordDomainDataDecoder.class);
        rheosConsumerWrapper = mock(RheosConsumerWrapper.class);
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
        MockProducer<Long, BehaviorMessage> behaviorProducer = new MockProducer<>(true, new LongSerializer(), new BehaviorMessageSerializer());
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

        //natural search click events
        RheosEvent rheosEvent1 = getRecord("3085", "roverns");
        GenericRecord genericRecord1 = mock(GenericRecord.class);
        when(decoder.decode(rheosEvent1)).thenReturn(genericRecord1);
        HashMap<Utf8, Utf8> data1 = new HashMap<Utf8, Utf8>();
        data1.put(new Utf8("urlQueryString"), new Utf8("/roverns/1/711-13271-9788-0?showdebug=async&mpt=1372189049793&mpcl=http%3A%2F%2Fwww.qa.ebay.com%2F&mpvl=http%3A%2F%2Fa+.yhs4.search.yahoo.com"));
        data1.put(new Utf8("ForwardFor"), new Utf8("10.149.170.138"));
        data1.put(new Utf8("referrer"), new Utf8("http://a+.yhs4.search.yahoo.com"));

        HashMap<Utf8, Utf8> data2 = new HashMap<Utf8, Utf8>();
        data2.put(new Utf8("chnl"), new Utf8("3"));
        data2.put(new Utf8("rvrid"), new Utf8("12345"));
        data2.put(new Utf8("timestamp"), new Utf8("1573445422467"));

        when(genericRecord1.get("clientData")).thenReturn(data1);
        when(genericRecord1.get("applicationPayload")).thenReturn(data2);
        when(genericRecord1.get("eventTimestamp")).thenReturn(1234567L);
        when(genericRecord1.get("pageId")).thenReturn(3085);
        when(genericRecord1.get("guid")).thenReturn("59b405c216e0a4e287dc0e85ffff607d");

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

        //send data to rheos
        consumerMcs.addRecord(new ConsumerRecord<byte[], RheosEvent>("test_topic", 0, 0L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) rheosEvent1.get("eventTimestamp")).array(), rheosEvent1));

        consumerMcs.addRecord(new ConsumerRecord<byte[], RheosEvent>("test_topic", 0, 1L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) rheosEvent2.get("eventTimestamp")).array(), rheosEvent2));

        consumerMcs.addRecord(new ConsumerRecord<byte[], RheosEvent>("test_topic", 0, 2L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) rheosEvent3.get("eventTimestamp")).array(), rheosEvent3));


        //verify open event
        roverRheosTopicFilterTask.processRecords(rheosConsumerWrapper, producer, behaviorProducer, "");
        Thread.sleep(5000);
        List<ProducerRecord<Long, ListenerMessage>> history = producer.history();
        assertEquals(3, history.size());
    }

    private RheosEvent createRheosEvent(int pageId, String pageName, String chnl, String urlQueryString) {
        RheosEvent roveropen = getRecord(String.valueOf(pageId), pageName);
        GenericRecord genericRecord = mock(GenericRecord.class);
        when(decoder.decode(roveropen)).thenReturn(genericRecord);

        when(genericRecord.get("clientData")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
                put(new Utf8("urlQueryString"), new Utf8(urlQueryString));
                put(new Utf8("ForwardFor"), new Utf8("10.149.170.138"));
                put(new Utf8("referrer"), new Utf8("http://a+.yhs4.search.yahoo.com"));
            }
        });
        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
                put(new Utf8("chnl"), new Utf8(chnl));
                put(new Utf8("rvrid"), new Utf8("12345"));
                put(new Utf8("timestamp"), new Utf8("1573445422467"));
            }
        });

        when(genericRecord.get("eventTimestamp")).thenReturn(1234567L);
        when(genericRecord.get("pageId")).thenReturn(pageId);
        when(genericRecord.get("pageName")).thenReturn("roveropen");
        when(genericRecord.get("guid")).thenReturn("59b405c216e0a4e287dc0e85ffff607d");
        return roveropen;
    }

    @Test
    public void testProcessEmailOpenRecords() throws Exception {
        initConsumer("behavior.pulsar.customized.page3962");

        RheosEvent roveropen1 = createRheosEvent(3962, "roveropen", "7",
                "/roveropen/0/e12060/7?osub=-1%7E1&crd=20200813220048&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869");

        RheosEvent roveropen2 = createRheosEvent(3963, "roveropen", "7",
                "/roveropen/0/e12060/7?osub=-1%7E1&crd=20200813220048&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869");

        RheosEvent roveropen3 = createRheosEvent(3962, "impression", "7",
                "/roveropen/0/e12060/7?osub=-1%7E1&crd=20200813220048&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869");

        RheosEvent roveropen4 = createRheosEvent(3962, "roveropen", "",
                "/roveropen/0/e12060/7?osub=-1%7E1&crd=20200813220048&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869");

        RheosEvent roveropen5 = createRheosEvent(3962, "roveropen", "",
                "/roveropen/0/e12060/?osub=-1%7E1&crd=20200813220048&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869");

        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.customized.page3962", 0, 0L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen1.get("eventTimestamp")).array(), roveropen1));
        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.customized.page3962", 0, 1L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen2.get("eventTimestamp")).array(), roveropen2));
        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.customized.page3962", 0, 2L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen3.get("eventTimestamp")).array(), roveropen3));
        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.customized.page3962", 0, 3L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen4.get("eventTimestamp")).array(), roveropen4));
        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.customized.page3962", 0, 4L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen5.get("eventTimestamp")).array(), roveropen5));

        roverRheosTopicFilterTask.processRecords(rheosConsumerWrapper, producer, behaviorProducer, "");

        Thread.sleep(5000);
        List<ProducerRecord<Long, BehaviorMessage>> history = behaviorProducer.history();

        history.forEach(record -> {
            assertEquals("EMAIL_OPEN", history.get(0).value().getChannelAction());
            assertEquals("Rover_Open", history.get(0).value().getPageName());
            assertEquals("SITE_EMAIL", history.get(0).value().getChannelType());
        });
    }

    private void initConsumer(String topic) {
        when(rheosConsumerWrapper.getConsumer()).thenReturn(consumerMcs);
        when(rheosConsumerWrapper.getDecoder()).thenReturn(decoder);
        consumerMcs.assign(Collections.singletonList(new TopicPartition(topic, 0)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(topic, 0), 0L);
        consumerMcs.updateBeginningOffsets(beginningOffsets);
    }

    @Test
    public void testProcessEmailClickRecords() throws Exception {
        initConsumer("behavior.pulsar.customized.email");

        RheosEvent roveropen1 = createRheosEvent(2547208, "", "7",
                "/roveropen/0/e12060/7?osub=-1%7E1&crd=20200813220048&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869");

        RheosEvent roveropen2 = createRheosEvent(3084, "", "7",
                "/roveropen/0/e12060/7?osub=-1%7E1&crd=20200813220048&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869");

        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.customized.email", 0, 0L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen1.get("eventTimestamp")).array(), roveropen1));
        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.customized.email", 0, 1L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen2.get("eventTimestamp")).array(), roveropen2));

        roverRheosTopicFilterTask.processRecords(rheosConsumerWrapper, producer, behaviorProducer, "");

        Thread.sleep(5000);
        List<ProducerRecord<Long, BehaviorMessage>> history = behaviorProducer.history();

        assertEquals("CLICK", history.get(0).value().getChannelAction());
        assertEquals(Integer.valueOf(2547208), history.get(0).value().getPageId());
        assertEquals("Rover_Click", history.get(1).value().getPageName());
        assertEquals("SITE_EMAIL", history.get(0).value().getChannelType());

        assertEquals("CLICK", history.get(1).value().getChannelAction());
        assertEquals(Integer.valueOf(3084), history.get(1).value().getPageId());
        assertEquals("Rover_Click", history.get(1).value().getPageName());
        assertEquals("SITE_EMAIL", history.get(1).value().getChannelType());
    }

    @Test
    public void testProcessBotRecords() throws Exception {
        initConsumer("behavior.pulsar.misc.bot");

        RheosEvent chocolateClick = createRheosEvent(2547208, "", "7", "");

        RheosEvent roverClick = createRheosEvent(3084, "", "7", "");

        RheosEvent roveropen = createRheosEvent(3962, "roveropen", "7", "");

        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 0L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) chocolateClick.get("eventTimestamp")).array(), chocolateClick));
        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 1L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roverClick.get("eventTimestamp")).array(), roverClick));
        consumerMcs.addRecord(new ConsumerRecord<>("behavior.pulsar.misc.bot", 0, 2L,
                ByteBuffer.allocate(Long.BYTES).putLong((long) roveropen.get("eventTimestamp")).array(), roveropen));

        roverRheosTopicFilterTask.processRecords(rheosConsumerWrapper, producer, behaviorProducer, "");

        Thread.sleep(5000);
        List<ProducerRecord<Long, BehaviorMessage>> history = behaviorProducer.history();

        assertEquals("CLICK", history.get(0).value().getChannelAction());
        assertEquals(Integer.valueOf(2547208), history.get(0).value().getPageId());
        assertEquals("Chocolate_Click_Bot", history.get(0).value().getPageName());
        assertEquals("SITE_EMAIL", history.get(0).value().getChannelType());

        assertEquals("CLICK", history.get(1).value().getChannelAction());
        assertEquals(Integer.valueOf(3084), history.get(1).value().getPageId());
        assertEquals("Rover_Click_Bot", history.get(1).value().getPageName());
        assertEquals("SITE_EMAIL", history.get(1).value().getChannelType());

        assertEquals("EMAIL_OPEN", history.get(2).value().getChannelAction());
        assertEquals(Integer.valueOf(3962), history.get(2).value().getPageId());
        assertEquals("Rover_Open_Bot", history.get(2).value().getPageName());
        assertEquals("SITE_EMAIL", history.get(2).value().getChannelType());
    }

    @Test
    public void parseChannelTypeFromUrlQueryString() {
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("")));
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/10?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertNull(roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertEquals("MRKT_EMAIL", roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/8?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
        assertEquals("SITE_EMAIL", roverRheosTopicFilterTask.parseChannelType(new Utf8("/roveropen/0/e12060/7?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869")));
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

        assertEquals("SITE_EMAIL", roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
                put(new Utf8("chnl"), new Utf8("8"));
            }
        });

        assertEquals("MRKT_EMAIL", roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("applicationPayload")).thenReturn(new HashMap<Utf8, Utf8>() {
            {
            }
        });
        when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen/0/e12060/8?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869"));

        assertEquals("MRKT_EMAIL", roverRheosTopicFilterTask.parseChannelType(genericRecord));

        when(genericRecord.get("urlQueryString")).thenReturn(new Utf8("/roveropen/0/e12060/7?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869"));

        assertEquals("SITE_EMAIL", roverRheosTopicFilterTask.parseChannelType(genericRecord));
    }
}
