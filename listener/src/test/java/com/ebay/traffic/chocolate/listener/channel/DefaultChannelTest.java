package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.TestHelper;
import com.ebay.traffic.chocolate.listener.util.*;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.net.ssl.*", "javax.security.*" })
@PrepareForTest({ListenerOptions.class, ESMetrics.class, MessageObjectParser.class, KafkaSink.class})
public class DefaultChannelTest {
  private MockHttpServletRequest mockClientRequest;
  private MessageObjectParser mockMessageParser;
  private Metrics mockMetrics;
  private MockHttpServletResponse mockProxyResponse;
  private Producer mockProducer;
  private DefaultChannel channel;
  private static CouchbaseClient couchbaseClient;

  @BeforeClass
  public static void beforeClass() throws Exception{
    CouchbaseClientMock.createClient();
    ESMetrics.init("test", "localhost");

    CacheFactory cacheFactory = Mockito.mock(CacheFactory.class);
    BaseDelegatingCacheClient baseDelegatingCacheClient = Mockito.mock(BaseDelegatingCacheClient.class);
    Couchbase2CacheClient couchbase2CacheClient = Mockito.mock(Couchbase2CacheClient.class);
    when(couchbase2CacheClient.getCouchbaseClient()).thenReturn(CouchbaseClientMock.getBucket());
    when(baseDelegatingCacheClient.getCacheClient()).thenReturn(couchbase2CacheClient);
    when(cacheFactory.getClient(any())).thenReturn(baseDelegatingCacheClient);

    couchbaseClient = new CouchbaseClient(cacheFactory);
    CouchbaseClient.init(couchbaseClient);
  }

  @Before
  public void setUp() throws Exception {
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
    mockMetrics = mock(Metrics.class);
    PowerMockito.mockStatic(ESMetrics.class);
    PowerMockito.when(ESMetrics.getInstance()).thenReturn(mockMetrics);
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
    long actual = channel.getCampaignID(mockClientRequest, System.currentTimeMillis(), "click", "epn");
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdShouldRaiseExceptionWhenCampIdInvalid() {
    long expected = 5331L;
    Map<String, String[]> params = new HashMap<>();
    params.put("campid", new String[] {"5331AQWAA765"});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest, System.currentTimeMillis(), "click", "epn");
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdIsCaseInsensitiveAndShouldReturnValueWhenUpperCase() {
    long expected = 5337991765L;
    Map<String, String[]> params = new HashMap<>();
    params.put("CAmpID", new String[] {"5337991765"});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest, System.currentTimeMillis(), "click", "epn");
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdShouldReturnDefaultIfNoCampaignFound() {
    long expected = -1L;
    Map<String, String[]> params = new HashMap<>();
    params.put("campidxyz", new String[] {"5337991765"});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest, System.currentTimeMillis(), "click", "epn");
    assertEquals(expected, actual);
  }

  @Test
  public void campaignIdShouldReturnDefaultWhenNoCampaignIdFound() {
    long expected = -1L;
    Map<String, String[]> params = new HashMap<>();
    params.put("caMpid", new String[] {""});
    params.put("a", new String[] {"b"});
    mockClientRequest.setParameters(params);
    long actual = channel.getCampaignID(mockClientRequest, System.currentTimeMillis(), "click", "epn");
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
    when(mockMessageParser.isCoreSite(any())).thenReturn(true);
    when(mockMessage.getSnapshotId()).thenReturn(snapshotId);
    when(mockMessage.toString()).thenReturn(kafkaMessage);
    when(mockMessage.getUri()).thenReturn("http://rover.ebay.com/roverimp/1/xyz/1");

    spy.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(1)).send(new ProducerRecord<>("epn", snapshotId, mockMessage), KafkaSink.callback);

    mockClientRequest.setServerName("rover.ebay.co.uk");
    when(mockMessageParser.isCoreSite(any())).thenReturn(false);
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
    when(mockMessageParser.isCoreSite(any())).thenReturn(true);
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
        eq(ChannelType.EPN), eq(ChannelActionEnum.IMPRESSION), anyString(), anyString()))
        .thenReturn(mockMessage);
    when(mockMessageParser.getMethod(mockClientRequest)).thenReturn(HttpMethodEnum.PUT);
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(1)).send(new ProducerRecord<>("listened-filtered", anyLong(), anyObject()), KafkaSink.callback);
  }

  @Test
  public void processShouldNotSendMessageToKafkaOrJournalIfInvalidCampaign() {
    ListenerMessage mockMessage = mock(ListenerMessage.class);
    when(mockMessageParser.parseHeader(eq(mockClientRequest), eq(mockProxyResponse), anyLong(), anyLong(),
        eq(ChannelType.EPN), eq(ChannelActionEnum.IMPRESSION), anyString(), anyString()))
        .thenReturn(mockMessage);
    when(mockMessageParser.getMethod(mockClientRequest)).thenReturn(HttpMethodEnum.PUT);
    // with negative number
    mockClientRequest.setParameter("campid",String.valueOf(-1234L));
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(1)).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);

    // with string
    mockClientRequest.setParameter("campid","12345abcde");
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(2)).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);

    // without campid tag
    mockClientRequest.setParameter("campxid","12345");
    channel.process(mockClientRequest, mockProxyResponse);
    verify(mockProducer, times(3)).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
  }

  @Test
  public void processRoverSyncCommand() throws Exception{
    String setCookie = "npii=btguid/75866c251690ab6af110f0bffffdb2535e69c89c^cguid/758674e91690a99b9634d24de79bcaad5e69c89c^;Domain=.ebay.co.uk;Expires=Thu, 12-Mar-2020 05:29:00 GMT;Path=/";
    mockClientRequest.setRequestURI("rover.ebay.co.uk/roversync/?site=3&stg=1&cguid=758674e91690a99b9634d24de79bcaad&mpt=1552454939881");
    mockProxyResponse.setHeader("Set-Cookie", setCookie);
    when(mockMessageParser.getGuid(any(), any(), any(), eq("cguid"))).thenReturn("758674e91690a99b9634d24de79bcaad");
    when(mockMessageParser.getGuid(any(), any(), any(), eq("tguid"))).thenReturn("75866c251690ab6af110f0bffffdb253");
    channel.process(mockClientRequest, mockProxyResponse);
    Thread.sleep(3000);
    assertEquals("758674e91690a99b9634d24de79bcaad", CouchbaseClient.getInstance().getCguid("75866c251690ab6af110f0bffffdb253"));
  }


  @Test
  public void processUpsertChocoTagGuidMappingForClick() throws Exception{
      final long campaignId = TestHelper.positiveLong();
      final long snapshotId = TestHelper.positiveLong();
      Map<String, String[]> params = new HashMap<>();
      params.put("CaMpid", new String[] {String.valueOf(campaignId)});
      params.put("a", new String[] {"b"});
      mockClientRequest.setParameters(params);
      mockClientRequest.setMethod("get");
      mockClientRequest.setRemoteHost("rover.ebay.com");
      mockClientRequest.setRequestURI("/rover/1/xyz/1");
      String kafkaMessage = "fake kafka message (EPNChannelTest)";

      DefaultChannel spy = spy(channel);
      ListenerMessage mockMessage = mock(ListenerMessage.class);

      when(mockMessageParser.parseHeader(eq(mockClientRequest), eq(mockProxyResponse), anyLong(), eq(campaignId),
              eq(ChannelType.EPN), eq(ChannelActionEnum.CLICK), anyString(), anyString())).thenReturn(mockMessage);
      when(mockMessageParser.isCoreSite(any())).thenReturn(true);
      when(mockMessage.getSnapshotId()).thenReturn(snapshotId);
      when(mockMessage.toString()).thenReturn(kafkaMessage);
      when(mockMessage.getUri()).thenReturn("http://rover.ebay.com/rover/1/xyz/1");
      when(mockMessageParser.getGuid(any(), any(), any(), eq("tguid"))).thenReturn("75866c251690ab6af110f0bffffdb253");
      when(mockMessageParser.getChocoTagValue(anyString())).thenReturn("12345678");

      spy.process(mockClientRequest, mockProxyResponse);

      Thread.sleep(3000);
      assertEquals("12345678", CouchbaseClient.getInstance().getChocoTagGuidMappingByChocoTag("DashenId_12345678").get("chocoTag"));
      assertEquals("75866c251690ab6af110f0bffffdb253", CouchbaseClient.getInstance().getChocoTagGuidMappingByChocoTag("DashenId_12345678").get("guid"));
  }

  @Test
  public void processUpsertChocoTagGuidMappingForImpression() throws Exception{
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
      when(mockMessageParser.isCoreSite(any())).thenReturn(true);
      when(mockMessage.getSnapshotId()).thenReturn(snapshotId);
      when(mockMessage.toString()).thenReturn(kafkaMessage);
      when(mockMessage.getUri()).thenReturn("http://rover.ebay.com/roverimp/1/xyz/1");
      when(mockMessageParser.getGuid(any(), any(), any(), eq("tguid"))).thenReturn("75866c251690ab6af110f0bffffdb253");
      when(mockMessageParser.getChocoTagValue(anyString())).thenReturn("1234567");

      spy.process(mockClientRequest, mockProxyResponse);

      Thread.sleep(3000);
      // no upsert for impression
      Map<String, String> chocotagGuidMap = new HashMap<>();
      assertEquals(chocotagGuidMap, CouchbaseClient.getInstance().getChocoTagGuidMappingByChocoTag("DashenId_1234567"));
    }
}
