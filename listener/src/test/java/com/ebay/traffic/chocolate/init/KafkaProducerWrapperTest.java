package com.ebay.traffic.chocolate.init;

import com.ebay.traffic.chocolate.init.KafkaProducerWrapper;
import com.ebay.traffic.chocolate.listener.TestHelper;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class KafkaProducerWrapperTest {
    private static Producer mockProducer;
    private static KafkaProducerWrapper kafka;

    @SuppressWarnings("unchecked") // for mocking generics
    @Before
    public void setUp() {
        mockProducer = mock(Producer.class);
        kafka = new KafkaProducerWrapper("test", mockProducer);
    }

    @After
    public void tearDown() {
        kafka.close();
        verify(mockProducer, times(1)).flush();
        verify(mockProducer, times(1)).close();
    }

    @Test
    public void producerInstanceShouldNotBeNullAfterInit() {
        assertNotNull(kafka);
    }

    @Test
    public void producerShouldProduceMeaningfulStringRepresentation() {
        assertEquals("topic=test producer_class=org.apache.kafka.clients.producer",
                kafka.toString().substring(0, 59)); // the final class name varies in a mock
    }

    @SuppressWarnings("unchecked") // for mocking generics
    @Test
    public void callingSendShouldCallSendOnUnderlyingProducer() {
        long campaignId = TestHelper.positiveLong();
        String message = new String(TestHelper.entry);
        ProducerRecord<Long, String> record = new ProducerRecord<>("test", campaignId, message);
        kafka.send(campaignId, message);
        verify(mockProducer, times(1)).send(eq(record), anyObject());
    }

    @Test
    public void callingReplayShouldCallSendWithCorrectArguments() {
        long campaignId = TestHelper.positiveLong();
        long snapshotId = TestHelper.positiveLong();
        byte[] entry = ByteBuffer.allocate(Long.BYTES + TestHelper.entry.length).putLong(campaignId).put(TestHelper.entry).array();
        KafkaProducerWrapper spy = spy(kafka);

        spy.replay(snapshotId, entry);

        verify(spy, times(1)).send(campaignId, new String(TestHelper.entry));
    }
}