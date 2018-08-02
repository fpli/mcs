package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.TestHelper;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ListenerOptions.class, MetricsClient.class, ESMetrics.class, MessageObjectParser.class, KafkaSink.class})
public class DefaultChannelTest {
  private MockHttpServletRequest mockClientRequest;
  private MessageObjectParser mockMessageParser;
  private MetricsClient mockMetrics;
  private ESMetrics mockESMetrics;
  private MockHttpServletResponse mockProxyResponse;
  private Producer mockProducer;
  private DefaultChannel channel;


  @Before
  public void setUp() {
   ListenerOptions mockOptions = mock(ListenerOptions.class);
   PowerMockito.mockStatic(ListenerOptions.class);
   PowerMockito.when(ListenerOptions.getInstance()).thenReturn(mockOptions);

   Map<ChannelType, String> channelTopics = new HashMap<>();
   channelTopics.put(ChannelType.EPN, "epn");
   channelTopics.put(ChannelType.DISPLAY, "display");
   PowerMockito.when(mockOptions.getSinkKafkaConfigs()).thenReturn(channelTopics);
   when(mockOptions.getListenerFilteredTopic()).thenReturn("listened-filtered");
   mockProducer = mock(KafkaProducer.class);
   PowerMockito.mockStatic(KafkaSink.class);
   PowerMockito.when(KafkaSink.get()).thenReturn(mockProducer);
   mockMetrics = mock(MetricsClient.class);
   PowerMockito.mockStatic(MetricsClient.class);
   PowerMockito.when(MetricsClient.getInstance()).thenReturn(mockMetrics);
   mockESMetrics = mock(ESMetrics.class);
   PowerMockito.mockStatic(ESMetrics.class);
   PowerMockito.when(ESMetrics.getInstance()).thenReturn(mockESMetrics);
   mockMessageParser = mock(MessageObjectParser.class);
   PowerMockito.mockStatic(MessageObjectParser.class);
   PowerMockito.when(MessageObjectParser.getInstance()).thenReturn(mockMessageParser);
   mockClientRequest = new MockHttpServletRequest();
   mockProxyResponse = new MockHttpServletResponse();
   channel = new DefaultChannel();
  }


  @Test
  public void campaignIdShouldReturnWhenPresentInQueryString() {
    long expected = 5337991765L;
    Map<String, String[]> params = new HashMap<>();
    params.put("campid", new String[] {"5337991765"});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest);
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdShouldRaiseExceptionWhenCampIdInvalid() {
    long expected = -1L;
    Map<String, String[]> params = new HashMap<>();
    params.put("campid", new String[] {"5331AQWAA765"});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest);
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdIsCaseInsensitiveAndShouldReturnValueWhenUpperCase() {
    long expected = 5337991765L;
    Map<String, String[]> params = new HashMap<>();
    params.put("CAmpID", new String[] {"5337991765"});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest);
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdShouldReturnDefaultIfNoCampaignFound() {
    long expected = -1L;
    Map<String, String[]> params = new HashMap<>();
    params.put("campidxyz", new String[] {"5337991765"});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest);
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdShouldReturnDefaultWhenNoCampaignIdFound() {
    long expected = -1L;
    Map<String, String[]> params = new HashMap<>();
    params.put("caMpid", new String[] {""});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest);
    assertEquals(expected, actual);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void processShouldSendCorrectMessageToKafkaWithEPNImpressionEvent() throws Exception{
    final long campaignId = TestHelper.positiveLong();
    final long snapshotId = TestHelper.positiveLong();
    Map<String, String[]> params = new HashMap<>();
    params.put("CaMpid", new String[] {String.valueOf(campaignId)});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    mockClientRequest.setMethod("get");
    mockClientRequest.setServerName("rover.ebay.com");
    mockClientRequest.setRequestURI("/roverimp/1/xyz/1");
    String kafkaMessage = "fake kafka message (EpnChannelTest)";

    DefaultChannel spy = spy(channel);
    ListenerMessage mockMessage = mock(ListenerMessage.class);

    when(mockMessageParser.parseHeader(eq(mockClientRequest), eq(mockProxyResponse), anyLong(), eq(campaignId),
        eq(ChannelType.EPN), eq(ChannelActionEnum.IMPRESSION), anyString(), anyString())).thenReturn(mockMessage);
    when(mockMessageParser.isCoreSite(anyString())).thenReturn(true);
    when(mockMessage.getSnapshotId()).thenReturn(snapshotId);
    when(mockMessage.toString()).thenReturn(kafkaMessage);
    when(mockMessage.getUri()).thenReturn("http://rover.ebay.com/roverimp/1/xyz/1");

    spy.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(1)).send(new ProducerRecord<>("epn", snapshotId, mockMessage), KafkaSink.callback);

    when(mockMessageParser.isCoreSite(anyString())).thenReturn(false);
    spy.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(1)).send(new ProducerRecord<>("listened-filtered", snapshotId, mockMessage), KafkaSink.callback);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void processShouldSendCorrectMessageToKafkaWithDAPClickEvent() throws Exception{
    final long campaignId = -1L;
    final long snapshotId = TestHelper.positiveLong();
    Map<String, String[]> params = new HashMap<>();
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    mockClientRequest.setMethod("get");
    mockClientRequest.setRemoteHost("rover.ebay.com");
    mockClientRequest.setRequestURI("/rover/1/xyz/4");
    String kafkaMessage = "fake kafka message (DAPChannelTest)";

    DefaultChannel spy = spy(channel);
    ListenerMessage mockMessage = mock(ListenerMessage.class);

    when(mockMessageParser.parseHeader(eq(mockClientRequest), eq(mockProxyResponse), anyLong(), eq(campaignId),
        eq(ChannelType.DISPLAY), eq(ChannelActionEnum.CLICK), anyString(), anyString())).thenReturn(mockMessage);
    when(mockMessageParser.isCoreSite(anyString())).thenReturn(true);
    when(mockMessage.getSnapshotId()).thenReturn(snapshotId);
    when(mockMessage.toString()).thenReturn(kafkaMessage);
    when(mockMessage.getUri()).thenReturn("http://rover.ebay.com/rover/1/xyz/4");

    spy.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(1)).send(new ProducerRecord<>("display", snapshotId, mockMessage), KafkaSink.callback);
  }

  @Test
  public void processShouldNotSendMessageToKafkaOrJournalIfItCouldNotBeParsed() {
    ListenerMessage mockMessage = mock(ListenerMessage.class);
    when(mockMessageParser.parseHeader(eq(mockClientRequest), eq(mockProxyResponse), anyLong(), anyLong(),
        eq(ChannelType.EPN), eq(ChannelActionEnum.IMPRESSION), eq(""), eq(null)))
        .thenReturn(mockMessage);
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, never()).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
  }

  @Test
  public void processShouldNotSendMessageToKafkaOrJournalIfInvalidCampaign() {
    // with negative number
    mockClientRequest.setParameter("campid",String.valueOf(-1234L));
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, never()).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);

    // with string
    mockClientRequest.setParameter("campid","12345abcde");
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, never()).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);

    // without campid tag
    mockClientRequest.setParameter("campxid","12345");
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, never()).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
  }

}
