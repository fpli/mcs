package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.cratchit.server.Page;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.TestHelper;
import com.ebay.traffic.chocolate.listener.util.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ListenerOptions.class, MetricsClient.class, MessageObjectParser.class, KafkaSink.class})
public class EpnChannelTest {
    private MockHttpServletRequest mockClientRequest;
    private EpnChannel channel;
    private MessageObjectParser mockMessageParser;
    private MetricsClient mockMetrics;
    private MockHttpServletResponse mockProxyResponse;
    private Producer mockProducer;

    @Before
    public void setUp() throws IOException, ExecutionException, InterruptedException {
        ListenerOptions mockOptions = mock(ListenerOptions.class);
        PowerMockito.mockStatic(ListenerOptions.class);
        PowerMockito.when(ListenerOptions.getInstance()).thenReturn(mockOptions);
        Map<ChannelType, String> channelTopics = new HashMap<>();
        channelTopics.put(ChannelType.EPN, "epn");
        PowerMockito.when(mockOptions.getSinkKafkaConfigs()).thenReturn(channelTopics);

        mockProducer = mock(org.apache.kafka.clients.producer.KafkaProducer.class);
        PowerMockito.mockStatic(KafkaSink.class);
        PowerMockito.when(KafkaSink.get()).thenReturn(mockProducer);
        mockMetrics = mock(MetricsClient.class);
        PowerMockito.mockStatic(MetricsClient.class);
        PowerMockito.when(MetricsClient.getInstance()).thenReturn(mockMetrics);
        mockMessageParser = mock(MessageObjectParser.class);
        PowerMockito.mockStatic(MessageObjectParser.class);
        PowerMockito.when(MessageObjectParser.getInstance()).thenReturn(mockMessageParser);
        mockClientRequest = new MockHttpServletRequest();
        mockProxyResponse = new MockHttpServletResponse();

        channel = new EpnChannel(ChannelActionEnum.IMPRESSION, LogicalChannelEnum.EPN);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void partitionKeyShouldReturnCampaignIdWhenPresentInQueryString() {
        long expected = 5337991765L;
        mockClientRequest.setQueryString("campid=" + expected + "&a=b");

        long actual = channel.getPartitionKey(mockClientRequest);
        assertEquals(expected, actual);
    }

    @Test
    public void partitionKeyIsCaseInsensitiveAndShouldReturnValueWhenUpperCase() {
        long expected = 5337991765L;

        mockClientRequest.setQueryString("a=b&CAMPID=" + expected);

        long actual = channel.getPartitionKey(mockClientRequest);
        assertEquals(expected, actual);
    }

    @Test
    public void partitionKeyShouldReturnErrorIfNoCampaignIdFound() {
        mockClientRequest.setQueryString("a=b&CAMPID=unused");
        long actual = channel.getPartitionKey(mockClientRequest);
        assertEquals(Channel.PARTITION_KEY_ERROR, actual);
    }

    @Test
    public void partitionKeyShouldReturnErrorIfNoCampaignFound() {
        mockClientRequest.setQueryString("a=b&campidxyz=12345");
        long actual = channel.getPartitionKey(mockClientRequest);
        assertEquals(Channel.PARTITION_KEY_ERROR, actual);
    }

    @Test
    public void partitionKeyShouldReturnErrorWhenCampaignIdIsNotANumber() {
        mockClientRequest.setQueryString("a=b&Campid=12345abcde54321&c=d");
        long actual = channel.getPartitionKey(mockClientRequest);
        assertEquals(Channel.PARTITION_KEY_ERROR, actual);
    }

    @Test
    public void partitionKeyShouldReturnErrorWhenNoCampaignIdFound() {
        mockClientRequest.setQueryString("a=b&campId=&c=d");
        long actual = channel.getPartitionKey(mockClientRequest);
        assertEquals(Channel.PARTITION_KEY_ERROR, actual);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void processShouldSendCorrectMessageToKafkaAndJournal() throws Exception {
        final long campaignId = TestHelper.positiveLong();
        final long snapshotId = TestHelper.positiveLong();
        mockClientRequest.setQueryString("a=b&CAMPID=" + campaignId);
        mockClientRequest.setMethod("get");
        String kafkaMessage = "fake kafka message (EpnChannelTest)";
        String payload = new String(TestHelper.entry);
        byte[] expected = ByteBuffer.allocate(Long.BYTES + payload.getBytes().length).putLong(campaignId).put(payload.getBytes()).array();

        // set up mocks
        EpnChannel spy = spy(channel);
        ListenerMessage mockMessage = mock(ListenerMessage.class);
        Page mockPage = mock(Page.class);

        // set up stubs
        when(mockMessage.writeToJSON()).thenReturn(payload);
        when(mockMessageParser.responseShouldBeFiltered(mockClientRequest,mockProxyResponse)).thenReturn(false);
        when(mockMessageParser.parseHeader(eq(mockClientRequest), eq(mockProxyResponse), anyLong(), eq(campaignId),
                eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.IMPRESSION), anyString())).thenReturn(mockMessage);
        when(mockMessage.getSnapshotId()).thenReturn(snapshotId);
        when(mockMessage.toString()).thenReturn(kafkaMessage);
        when(mockPage.isActive()).thenReturn(true);
      //  when(spy.getJournalPage()).thenReturn(mockPage);

        spy.process(mockClientRequest, mockProxyResponse);

        verify(mockProducer, times(1)).send(new ProducerRecord<>("epn", snapshotId, mockMessage), KafkaSink.callback);
      //  verify(spy, times(1)).writeToJournal(mockPage, snapshotId, expected);
    }

    @Test
    public void processShouldNotSendMessageToKafkaOrJournalIfItCouldNotBeParsed() throws IOException {
        // set up mocks
        ListenerMessage mockMessage = mock(ListenerMessage.class);

        // set up stubs
        when(mockMessage.writeToJSON()).thenThrow(new IOException());
        when(mockMessageParser.parseHeader(eq(mockClientRequest), eq(mockProxyResponse), anyLong(), anyLong(),
                eq(LogicalChannelEnum.EPN.getAvro()), eq(ChannelActionEnum.IMPRESSION), eq("")))
                .thenReturn(mockMessage);

        channel.process(mockClientRequest, mockProxyResponse);

        verify(mockProducer, never()).send(new ProducerRecord<>("epn", anyLong(), anyObject()), KafkaSink.callback);
    }

    @Test
    public void processShouldNotSendMessageToKafkaOrJournalIfInvalidCampaign() throws IOException {
        // set up mocks
        ListenerMessage mockMessage = mock(ListenerMessage.class);

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

    /*@Test
    *public void journalPageIsInitializedWhenEnabled() throws IOException {
        EpnChannel.getNextJournalPage(true);
        assertNotNull(channel.getJournalPage());
        assertTrue(channel.getJournalPage().isActive());
    }*/

    @Test(expected=IllegalArgumentException.class)
    public void badChannelType() {
        new EpnChannel(ChannelActionEnum.IMPRESSION, LogicalChannelEnum.DISPLAY);
    }

    @Test(expected=IllegalArgumentException.class)
    public void badChannelAction() {
        new EpnChannel(null, LogicalChannelEnum.DISPLAY);
    }

    /*@Test
    public void journalPageIsNullWhenNotEnabled() throws IOException {
        EpnChannel.getNextJournalPage(false);
        assertNull(channel.getJournalPage());
    }

    @Test
    public void journalPageIsWrittenToWhenAMessageIsSent() {
        Page page = mock(Page.class);
        long snapshotId = TestHelper.positiveLong();
        byte[] entry = TestHelper.entry;

        when(page.isActive()).thenReturn(true);

        channel.writeToJournal(page, snapshotId, entry);

        verify(page).write(snapshotId, entry);
    }

    @Test
    public void newJournalPageRequestedWhenPageIsFull() throws IOException {
        EpnChannel spy = spy(channel);
        Page page1 = mock(Page.class);
        Page page2 = mock(Page.class);
        long snapshotId = TestHelper.positiveLong();
        byte[] entry = TestHelper.entry;

        doReturn(page2).when(spy).getJournalPage();
        when(page1.isActive()).thenReturn(true);
        when(page1.write(snapshotId, entry)).thenReturn((short) Journal.ERROR);

        when(page2.isActive()).thenReturn(true);
        when(page2.write(snapshotId, entry)).thenReturn((short) 4);

        spy.writeToJournal(page1, snapshotId, entry);

        verify(page1, times(1)).write(snapshotId, entry);
        verify(page2, times(1)).write(snapshotId, entry);
    }

    @Test
    public void newThreadShouldInitializeNewJournalPage() throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newSingleThreadExecutor();
        Future future = service.submit(new EpnChannelHandlerThread());
        future.get();
    }

    private class EpnChannelHandlerThread implements Runnable {

        private EpnChannel threadedHandler;

        @Override
        public void run() {
            threadedHandler = new EpnChannel(mockMessageParser, mockMetrics, mockKafka,
                    ChannelActionEnum.IMPRESSION, LogicalChannelEnum.EPN);
            assertNotNull(threadedHandler.getJournalPage());
            assertTrue(threadedHandler.getJournalPage().isActive());
        }
    }*/
}
