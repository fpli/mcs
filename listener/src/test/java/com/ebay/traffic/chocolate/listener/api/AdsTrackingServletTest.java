package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.LogicalChannelEnum;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.monitoring.Metrics;
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
public class AdsTrackingServletTest {

  private Metrics mockMetrics;
  private Producer mockProducer;
  private AdsTrackingServlet servlet;
  private MessageObjectParser mockParser;


  @Before
  public void setUp() {
    mockMetrics = mock(Metrics.class);
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

    servlet = new AdsTrackingServlet(mockMetrics, mockParser);
    servlet.init();
  }

  @Test
  public void testServletShouldQueryRequestURL() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://www.ebayadservices.com/adTracking/v1?mkevt=1&mkcid=1&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(-1L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq(null), eq(null))).thenReturn(mockObject);
    when(mockObject.getSnapshotId()).thenReturn(111L);
    when(mockObject.writeToJSON()).thenReturn("hello");
    Map<String, String[]> params = new HashMap<>();
    params.put("mklndp", new String[]{"http://www.ebay.com"});
    params.put("mkevt", new String[]{"1"});
    params.put("mkcid", new String[]{"1"});
    params.put("mkrid", new String[]{"711-1245-1245-235"});
    when(request.getParameterMap()).thenReturn(params);
    servlet.doGet(request, response);
//    AdsTrackingEvent event = new AdsTrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, atLeastOnce()).send(
        new ProducerRecord<>("epn", 111L, mockObject), KafkaSink.callback);
  }

  @Test
  public void testServletInvalidChannel() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://www.ebayadservices.com/adTracking/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&snid=foo");
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
  }

  @Test
  public void testServletInvalidCollectionId() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://www.ebayadservices.com/adTracking/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
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
  }

  @Test
  public void testServletInvalidMessage() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://www.ebayadservices.com/adTracking/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(-1l), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq(""), eq(null))).thenReturn(null);
    servlet.doGet(request, response);

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
  }

  @Test
  public void testServletInvalidJson() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://www.ebayadservices.com/adTracking/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);

    when(request.getRequestURL()).thenReturn(clickURL);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(12345L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.CLICK), eq(null), eq(null))).thenReturn(null);
    servlet.doGet(request, response);

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
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

    verify(request, atLeastOnce()).getRequestURL();
    verify(mockProducer, never()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
  }

  @Test
  public void testDoPostSuccessfullyWithEPNImpression() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://www.ebayadservices.com/adTracking/v1?mkevt=2&mkcid=4&mkrid=711-1245-1245-235&campid=7876756567&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);
    when(request.getRequestURL()).thenReturn(clickURL);

    Map<String, String[]> params = new HashMap<>();
    params.put("mklndp", new String[]{"http://www.ebay.com/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book"});
    params.put("mkevt", new String[]{"2"});
    params.put("mkcid", new String[]{"1"});
    params.put("mkrid", new String[]{"711-1245-1245-235"});
    params.put("campid", new String[]{"7876756567"});
    params.put("item", new String[]{"111111111"});

    when(request.getParameterMap()).thenReturn(params);
    ServletOutputStream mockOut = mock(ServletOutputStream.class);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(7876756567L), eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.IMPRESSION), eq(null), eq(null))).thenReturn(mockObject);
    when(response.getOutputStream()).thenReturn(mockOut);

    servlet.doPost(request, response);

    verify(mockProducer, atLeastOnce()).send(
        new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
  }

  @Test
  public void testDoPostSuccessfullyWithDAPClick() throws IOException {
    StringBuffer clickURL = new StringBuffer("https://www.ebayadservices.com/adTracking/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&campid=7876756567&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ListenerMessage mockObject = mock(ListenerMessage.class);
    when(request.getRequestURL()).thenReturn(clickURL);

    Map<String, String[]> map = new HashMap<>();
    map.put("mpt", new String[]{"dp3579964981464028849"});
    map.put("siteid", new String[]{"0"});
    map.put("ipn", new String[]{"admain2"});
    map.put("mpvc", new String[]{""});
    map.put("mklndp", new String[]{"http://www.ebay.com/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book"});
    map.put("mkevt", new String[]{"1"});
    map.put("mkcid", new String[]{"4"});
    map.put("mkrid", new String[]{"711-1245-1245-235"});
    map.put("campid", new String[]{"7876756567"});
    when(request.getParameterMap()).thenReturn(map);
    ServletOutputStream mockOut = mock(ServletOutputStream.class);
    when(mockParser.parseHeader(eq(request), eq(response), anyObject(), eq(7876756567L), eq(LogicalChannelEnum.DISPLAY.getAvro()), eq(ChannelActionEnum.CLICK), eq(null), eq(null))).thenReturn(mockObject);
    when(response.getOutputStream()).thenReturn(mockOut);

    servlet.doPost(request, response);
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());

    verify(mockProducer, atLeastOnce()).send(
        new ProducerRecord<>("display", anyLong(), anyObject()), KafkaSink.callback);
  }

}

