package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Most of the other test methods are in ChannelMapTest.java 
 * 
 * @author jepounds
 */
@SuppressWarnings("javadoc")
@RunWith(PowerMockRunner.class)
@PrepareForTest({ListenerOptions.class, KafkaSink.class})
@PowerMockIgnore( {"javax.management.*"})
public class ChannelFactoryTest {
    @Test
    public void testEpnChannel(){
        MockHttpServletRequest mock = new MockHttpServletRequest();
        Properties properties = new Properties();
        ListenerOptions.init(properties);
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "127.0.0.1:9092");
        kafkaProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        kafkaProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer mockProducer = mock(KafkaProducer.class);
        PowerMockito.mockStatic(KafkaSink.class);
        PowerMockito.when(KafkaSink.get()).thenReturn(mockProducer);

        mock.setRequestURI("/var/log/stuff/xyz");
        assertEquals(DefaultChannel.class, ChannelFactory.getChannel(mock).getClass());

        mock.setRequestURI("/roverroi/1/xyz/1");
        assertEquals(DefaultChannel.class, ChannelFactory.getChannel(mock).getClass());

        mock.setRequestURI("/rover/1/xyz/9");
        assertEquals(EpnChannel.class, ChannelFactory.getChannel(mock).getClass());

        mock.setRequestURI("/roverimp/1/xyz/1");
        assertEquals(EpnChannel.class, ChannelFactory.getChannel(mock).getClass());

        mock.setRequestURI("/roverimp/1/xyz/1/3");
        assertEquals(DefaultChannel.class, ChannelFactory.getChannel(mock).getClass());

        mock.setRequestURI("/roverimp/1/xyz/");
        assertEquals(DefaultChannel.class, ChannelFactory.getChannel(mock).getClass());

    }

    @Test
    public void testWarningMessageValid() {
        StringBuffer sb = new StringBuffer();
        sb.append("This is a test;");
        
        MockHttpServletRequest mock = new MockHttpServletRequest();
        mock.setProtocol("http");
        mock.setRequestURI("/var/log/stuff");
        mock.setQueryString("b=a");
        
        StringBuffer newSb = ChannelFactory.deriveWarningMessage(sb, mock);
        assertTrue(newSb == sb);
        String expected = "This is a test; URL=http://localhost/var/log/stuff queryStr=b=a";
        assertEquals(expected, newSb.toString());
    }
    
    @Test
    public void testWarningMessageNulls() {
        StringBuffer sb = new StringBuffer();
        sb.append("This is a test;");
        
        MockHttpServletRequest mock = new MockHttpServletRequest();
        mock.setProtocol("http");
        mock.setRequestURI("/var/log/stuff");
        mock.setQueryString(null);
        
        StringBuffer newSb = ChannelFactory.deriveWarningMessage(sb, mock);
        assertTrue(newSb == sb);
        String expected = "This is a test; URL=http://localhost/var/log/stuff queryStr=null";
        assertEquals(expected, newSb.toString());
    }
}
