package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.LogicalChannelEnum;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ListenerOptions.class, KafkaSink.class})
public class TrackingServletTest {

  private MetricsClient mockMetrics;
  private ESMetrics mockESMetrics;
  private Producer mockProducer;
  private TrackingServlet servlet;
  private MessageObjectParser mockParser;

  @Before
  public void setUp() {
    mockMetrics = mock(MetricsClient.class);
    mockESMetrics = mock(ESMetrics.class);
    mockParser = mock(MessageObjectParser.class);

    ListenerOptions mockOptions = mock(ListenerOptions.class);
    PowerMockito.mockStatic(ListenerOptions.class);
    PowerMockito.when(ListenerOptions.getInstance()).thenReturn(mockOptions);

    Map<ChannelType, String> channelTopics = new HashMap<>();
    channelTopics.put(ChannelType.EPN, "epn");
    channelTopics.put(ChannelType.DISPLAY, "display");
    PowerMockito.when(mockOptions.getSinkKafkaConfigs()).thenReturn(channelTopics);

    mockProducer = mock(org.apache.kafka.clients.producer.KafkaProducer.class);
    PowerMockito.mockStatic(KafkaSink.class);
    PowerMockito.when(KafkaSink.get()).thenReturn(mockProducer);

    servlet = new TrackingServlet(mockMetrics, mockESMetrics, mockParser);
    servlet.init();
  }

  @Test
  public void testServletShouldQueryRequestURL() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq(null), eq(null))).thenReturn(mockObject);
    when(mockObject.getSnapshotId()).thenReturn(111L);
    when(mockObject.writeToJSON()).thenReturn("hello");
    Map<String, String[]> params = new HashMap<>();
    params.put("page", new String[]{"http://www.ebay.com"});
    when(request.getParameterMap()).thenReturn(params);
    servlet.doGet(request, response);
    TrackingEvent event = new TrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, atLeastOnce()).send(
        new ProducerRecord<>("epn", 111L, mockObject), KafkaSink.callback);
    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, atLeastOnce()).meter("TrackingSuccess");
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingCount", event.getAction().toString(), event.getChannel().toString());
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingSuccess", event.getAction().toString(), event.getChannel().toString());
  }

  @Test
  public void testServletInvalidChannel() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/blabla-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&snid=foo");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq("foo"), eq(null))).thenReturn(mockObject);
    when(mockObject.writeToJSON()).thenReturn("hello");
    when(mockObject.getSnapshotId()).thenReturn(111L);
    servlet.doGet(request, response);

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockObject, never()).writeToJSON();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", 111L, mockObject), KafkaSink.callback);
    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, never()).meter("TrackingSuccess");
    //verify(mockESMetrics, never()).meter("TrackingCount", "CLICK", "EPN");
    //verify(mockESMetrics, never()).meter("TrackingSuccess", "CLICK", "EPN");
  }

  @Test
  public void testServletInvalidCollectionId() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-foobar?snid=bar&page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq("bar"), eq(null))).thenReturn(mockObject);
    when(mockObject.writeToJSON()).thenReturn("hello");
    when(mockObject.getSnapshotId()).thenReturn(111L);
    servlet.doGet(request, response);

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockObject, never()).writeToJSON();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", 111L, mockObject), KafkaSink.callback);
    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, never()).meter("TrackingSuccess");
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingCount", "CLICK", "EPN");
    //verify(mockESMetrics, never()).meter("TrackingSuccess", "CLICK", "EPN");
  }

  @Test
  public void testServletInvalidMessage() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq(""), eq(null))).thenReturn(null);
    servlet.doGet(request, response);
    TrackingEvent event = new TrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, never()).meter("TrackingSuccess");
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingCount", event.getAction().toString(), event.getChannel().toString());
    //verify(mockESMetrics, never()).meter("TrackingSuccess", event.getAction().toString(), event.getChannel().toString());
  }

  @Test
  public void testServletInvalidJson() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq(null), eq(null))).thenReturn(null);
    servlet.doGet(request, response);
    TrackingEvent event = new TrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, never()).meter("TrackingSuccess");
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingCount", event.getAction().toString(), event.getChannel().toString());
    //verify(mockESMetrics, never()).meter("TrackingSuccess", event.getAction().toString(), event.getChannel().toString());
  }

  @Test
  public void testServletRequestWithNoParams() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1i/1-7876756567");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ServletOutputStream mockOut = mock(ServletOutputStream.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(7876756567L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.IMPRESSION), eq(null), eq(null))).thenReturn(null);
    when(response.getOutputStream()).thenReturn(mockOut);
    servlet.doGet(request, response);
    TrackingEvent event = new TrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, never()).meter("TrackingSuccess");
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingCount", event.getAction().toString(), event.getChannel().toString());
    //verify(mockESMetrics, never()).meter("TrackingSuccess", event.getAction().toString(), event.getChannel().toString());
  }

  @Test
  public void testDoPostSuccessfullyWithEPNImpression() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1i/1-7876756567");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);
    when(request.getRequestURL()).thenReturn(clickURL);

    Map<String, String[]> map = new HashMap<>();
    map.put("page", new String[]{"http://www.ebay.com/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book"});
    map.put("item", new String[]{"111111111"});
    when(request.getParameterMap()).thenReturn(map);
    ServletOutputStream mockOut = mock(ServletOutputStream.class);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(7876756567L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.IMPRESSION), eq(null), eq(null))).thenReturn(mockObject);
    when(response.getOutputStream()).thenReturn(mockOut);

    servlet.doPost(request, response);
    TrackingEvent event = new TrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(mockProducer, atLeastOnce()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, atLeastOnce()).meter("TrackingSuccess");
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingCount", event.getAction().toString(), event.getChannel().toString());
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingSuccess", event.getAction().toString(), event.getChannel().toString());
  }

  @Test
  public void testDoPostSuccessfullyWithDAPClick() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://c.ebay.com/1c/4-7876756567");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);
    when(request.getRequestURL()).thenReturn(clickURL);

    Map<String, String[]> map = new HashMap<>();
    map.put("mpt", new String[]{"dp3579964981464028849"});
    map.put("siteid", new String[]{"0"});
    map.put("ipn", new String[]{"admain2"});
    map.put("mpvc", new String[]{""});
    map.put("page", new String[]{"http://www.ebay.com/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book"});
    when(request.getParameterMap()).thenReturn(map);
    ServletOutputStream mockOut = mock(ServletOutputStream.class);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(7876756567L), eq(LogicalChannelEnum.DISPLAY.getAvro()), eq(ChannelActionEnum.CLICK), eq(null), eq(null))).thenReturn(mockObject);
    when(response.getOutputStream()).thenReturn(mockOut);

    servlet.doPost(request, response);
    TrackingEvent event = new TrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(mockProducer, atLeastOnce()).send(
        new ProducerRecord<>("display", anyLong(), anyObject()), KafkaSink.callback);

    verify(mockMetrics, atLeastOnce()).meter("TrackingCount");
    verify(mockMetrics, atLeastOnce()).meter("TrackingSuccess");
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingCount", event.getAction().toString(), event.getChannel().toString());
    //verify(mockESMetrics, atLeastOnce()).meter("TrackingSuccess", event.getAction().toString(), event.getChannel().toString());
  }

}

