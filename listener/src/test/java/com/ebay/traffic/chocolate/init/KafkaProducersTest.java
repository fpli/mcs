package com.ebay.traffic.chocolate.init;

import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaProducersTest {
    private ListenerOptions options = mock(ListenerOptions.class);
    @Before
    public void setUp() throws Exception{
      HashMap<ChannelIdEnum, String> map = new HashMap<>();
      map.put(ChannelIdEnum.EPN, "EPN");
      map.put(ChannelIdEnum.DAP, "DAP");

      Properties kafkaProps = new Properties();
      kafkaProps.setProperty("bootstrap.servers", "127.0.0.1:9092");
      kafkaProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
      kafkaProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      when(options.getKafkaChannelTopicMap()).thenReturn(map);
      when(options.getKafkaProperties()).thenReturn(kafkaProps);
      KafkaProducers.init(options);
    }

    @After
    public void tearDown() {
        KafkaProducers.closeAll();
    }

    @Test
    public void testKafkaInit() {
        assertEquals(2, KafkaProducers.getInstance().getChannelProducerMap().size());
    }

    @Test
    public void testGetChannelProducerMap() {
        Map<ChannelIdEnum, KafkaProducerWrapper> result = KafkaProducers.getInstance().getChannelProducerMap();
        assertTrue(result != null);
        assertEquals(2, result.size());
        assertEquals("topic=EPN producer_class=org.apache.kafka.clients.producer.KafkaProducer",
                result.get(ChannelIdEnum.EPN).toString());
    }

    @Test
    public void testGetKafkaProducer() {
        assertEquals("topic=DAP producer_class=org.apache.kafka.clients.producer.KafkaProducer",
                KafkaProducers.getInstance().getKafkaProducer(ChannelIdEnum.DAP).toString());
    }


}
