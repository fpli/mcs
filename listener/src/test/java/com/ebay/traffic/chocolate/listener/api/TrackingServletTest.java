package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.init.KafkaProducerWrapper;
import com.ebay.traffic.chocolate.init.KafkaProducers;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.LogicalChannelEnum;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaProducers.class})
public class TrackingServletTest {
    
    private MetricsClient mockMetrics;
    private KafkaProducerWrapper mockKafka;
    private TrackingServlet servlet;
    private MessageObjectParser mockParser;

    @Before
    public void setUp() {
        mockMetrics = mock(MetricsClient.class);
        mockKafka = mock(KafkaProducerWrapper.class);
        mockParser = mock(MessageObjectParser.class);
        PowerMockito.mockStatic(KafkaProducers.class);
        KafkaProducers kafkaProducers = mock(KafkaProducers.class);
        PowerMockito.when(KafkaProducers.getInstance()).thenReturn(kafkaProducers);
        PowerMockito.when(kafkaProducers.getKafkaProducer(ChannelIdEnum.EPN)).thenReturn(mockKafka);

        servlet = new TrackingServlet(mockMetrics, mockParser);
        servlet.init();
    }

    @Test
    public void testServletShouldQueryRequestURL() throws IOException {
        StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ListenerMessage mockObject = mock(ListenerMessage.class);
        
        when(request.getRequestURL()).thenReturn(clickURL);
        when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN), eq(ChannelActionEnum.CLICK), eq(null))).thenReturn(mockObject);
        when(mockObject.writeToJSON()).thenReturn("hello");
        servlet.doGet(request, response);
        
        verify(request, atLeastOnce()).getRequestURL();
        verify(mockObject, atLeastOnce()).writeToJSON();
        verify(mockKafka, atLeastOnce()).send(12345L, "hello");
        verify(mockMetrics, atLeastOnce()).meter("VimpCount");
        verify(mockMetrics, atLeastOnce()).meter("VimpSuccess");
    }

    @Test
    public void testServletInvalidChannel() throws IOException {
        StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/blabla-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&snid=foo");
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ListenerMessage mockObject = mock(ListenerMessage.class);
        
        when(request.getRequestURL()).thenReturn(clickURL);
        when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN), eq(ChannelActionEnum.CLICK), eq("foo"))).thenReturn(mockObject);
        when(mockObject.writeToJSON()).thenReturn("hello");
        servlet.doGet(request, response);
        
        verify(request, atLeastOnce()).getRequestURL();
        verify(mockObject, never()).writeToJSON();
        verify(mockKafka, never()).send(12345L, "hello");
        verify(mockMetrics, atLeastOnce()).meter("VimpCount");
        verify(mockMetrics, never()).meter("VimpSuccess");
    }
    
    @Test
    public void testServletInvalidCollectionId() throws IOException {
        StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-foobar?snid=bar&page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ListenerMessage mockObject = mock(ListenerMessage.class);
        
        when(request.getRequestURL()).thenReturn(clickURL);
        when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN), eq(ChannelActionEnum.CLICK), eq("bar"))).thenReturn(mockObject);
        when(mockObject.writeToJSON()).thenReturn("hello");
        servlet.doGet(request, response);
        
        verify(request, atLeastOnce()).getRequestURL();
        verify(mockObject, never()).writeToJSON();
        verify(mockKafka, never()).send(12345L, "hello");
        verify(mockMetrics, atLeastOnce()).meter("VimpCount");
        verify(mockMetrics, never()).meter("VimpSuccess");
    }
    
    @Test
    public void testServletInvalidMessage() throws IOException {
        StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        when(request.getRequestURL()).thenReturn(clickURL);
        when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN), eq(ChannelActionEnum.CLICK), eq(""))).thenReturn(null);
        servlet.doGet(request, response);
        
        verify(request, atLeastOnce()).getRequestURL();
        verify(mockKafka, never()).send(12345L, "hello");
        verify(mockMetrics, atLeastOnce()).meter("VimpCount");
        verify(mockMetrics, never()).meter("VimpSuccess");
    }
    
    @Test
    public void testServletInvalidJson() throws IOException {
        StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ListenerMessage mockObject = mock(ListenerMessage.class);
        
        when(request.getRequestURL()).thenReturn(clickURL);
        when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN), eq(ChannelActionEnum.CLICK), eq(null))).thenReturn(mockObject);
        when(mockObject.writeToJSON()).thenReturn(null);
        servlet.doGet(request, response);
        
        verify(request, atLeastOnce()).getRequestURL();
        verify(mockObject, atLeastOnce()).writeToJSON();
        verify(mockKafka, never()).send(12345L, null);
        verify(mockMetrics, atLeastOnce()).meter("VimpCount");
        verify(mockMetrics, never()).meter("VimpSuccess");
    }

    @Test
    public void testServletRequestWithNoParams() throws IOException {
        StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1i/1-7876756567");
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream mockOut = mock(ServletOutputStream.class);

        ListenerMessage mockObject = mock(ListenerMessage.class);

        when(request.getRequestURL()).thenReturn(clickURL);
        when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(7876756567L), eq(LogicalChannelEnum.EPN), eq(ChannelActionEnum.IMPRESSION), eq(null))).thenReturn(mockObject);
        when(mockObject.writeToJSON()).thenReturn(null);
        when(response.getOutputStream()).thenReturn(mockOut);
        servlet.doGet(request, response);

        verify(request, atLeastOnce()).getRequestURL();
        verify(mockObject, atLeastOnce()).writeToJSON();
        verify(mockKafka, never()).send(7876756567L, null);
        verify(mockMetrics, atLeastOnce()).meter("VimpCount");
        verify(mockMetrics, never()).meter("VimpSuccess");
    }
}

