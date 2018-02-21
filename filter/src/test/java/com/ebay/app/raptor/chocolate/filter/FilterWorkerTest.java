package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;
import com.ebay.app.raptor.chocolate.filter.service.FilterContainer;
import com.ebay.app.raptor.chocolate.filter.service.FilterResult;
import com.ebay.app.raptor.chocolate.filter.service.FilterWorker;
import com.ebay.app.raptor.chocolate.filter.util.CampaignPublisherMappingCache;
import com.ebay.app.raptor.chocolate.filter.util.FilterZookeeperClient;
import com.ebay.raptor.test.framework.RaptorIOSpringRunner;
import org.ebayopensource.sherlock.client.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by spugach on 3/3/17.
 */
@RunWith(RaptorIOSpringRunner.class)
@SpringBootTest
public class FilterWorkerTest {
    private class MockKafka extends KafkaWrapper {
        public String inTopic = null;
        public String outTopic = null;
        public int closes = 0;
        public int polls = 0;
        public int sends = 0;

        public List<ListenerMessage> pollResult = null;

        @Override
        public void init(String inTopic, String outTopic) {
            this.inTopic = inTopic;
            this.outTopic = outTopic;
        }

        @Override
        public List<ListenerMessage> poll() {
            this.polls += 1;
            List<ListenerMessage> result = this.pollResult;
            this.pollResult = null;
            return result;
        }

        @Override
        public void send(FilterMessage message) {
            this.sends += 1;
        }

        @Override
        public void close() {
            this.closes += 1;
        }
    }

    private class MockFilterPass extends FilterContainer {
        public int testCalled = 0;

        @Override
        public FilterResult test(ListenerMessage request) {
            this.testCalled += 1;
            return new FilterResult(true, FilterRuleType.NONE);
        }
    }

    private class MockFilterFail extends FilterContainer {
        public int testCalled = 0;

        @Override
        public FilterResult test(ListenerMessage request) {
            this.testCalled += 1;
            throw new RuntimeException("test");
        }
    }

    @Before
    public void setUpKafka() {
        Properties properties = new Properties();
        properties.put(ApplicationOptions.KAFKA_IN_TOPIC, "foo");
        properties.put(ApplicationOptions.KAFKA_OUT_TOPIC, "bar");
        ApplicationOptions.init(properties);

        // Mock the MetricsClient stuff
        Session mockSession = mock(Session.class);
        Post mockPost = mock(Post.class);
        @SuppressWarnings("unchecked")
        ResultFuture<PostResult> mockFuture = mock(ResultFuture.class);

        when(mockSession.post(any(MetricSet.class))).thenReturn(mockPost);
        when(mockPost.execute()).thenReturn(mockFuture);
        if(MetricsClient.getInstance() == null){
            MetricsClient.init(mockSession);
        }
    }

    @After
    public void tearDown() {
      CampaignPublisherMappingCache.destroy();
      FilterZookeeperClient.getInstance().close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWorkerInvalidCreation() throws InterruptedException {
        FilterWorker worker = new FilterWorker(null, null, null);
    }

    @Test
    public void testWorkerShutsDown() throws InterruptedException {
        MockKafka k = new MockKafka();
        FilterWorker worker = new FilterWorker(new FilterContainer(), k, MetricsClient.getInstance());
        worker.start();
        Thread.sleep(200);
        assertEquals(true, worker.isAlive());
        worker.shutdown();
        Thread.sleep(200);
        assertEquals(false, worker.isAlive());
    }

    @Test
    public void testWorkerInitsAndClosesKafka() throws InterruptedException {
        MockKafka k = new MockKafka();
        FilterWorker worker = new FilterWorker(new FilterContainer(), k, MetricsClient.getInstance());
        worker.start();
        Thread.sleep(200);
        assertEquals("foo", k.inTopic);
        assertEquals("bar", k.outTopic);
        worker.shutdown();
        Thread.sleep(200);
        assertEquals(1, k.closes);
    }

    @Test
    public void testWorkerPollsKafkaWhenActive() throws InterruptedException {
        MockKafka k = new MockKafka();
        FilterWorker worker = new FilterWorker(new FilterContainer(), k, MetricsClient.getInstance());
        Thread.sleep(200);
        assertTrue(k.polls == 0);
        worker.start();
        Thread.sleep(200);
        assertTrue(k.polls > 0);
        worker.shutdown();
    }

    @Test
    public void testWorkerCallsFilteringAndOutputs() throws InterruptedException {
        MockKafka k = new MockKafka();
        k.pollResult = new ArrayList<ListenerMessage>();
        k.pollResult.add(new ListenerMessage());
        k.pollResult.add(new ListenerMessage());
        MockFilterPass f = new MockFilterPass();
        FilterWorker worker = new FilterWorker(f, k, MetricsClient.getInstance());
        worker.start();
        helperTimeoutCheck(() -> k.sends == 2 && f.testCalled == 2);
        worker.shutdown();
    }

    @Test
    public void testWorkerMessageProcessingCallsFilters() {
        MockFilterPass f = new MockFilterPass();
        ListenerMessage lm = new ListenerMessage();
        lm.setPublisherId(3L);
        lm.setCampaignId(4L);
        lm.setSnapshotId(1L);

        MockKafka k = new MockKafka();
        k.pollResult = new ArrayList<ListenerMessage>();
        FilterWorker worker = new FilterWorker(f, k, MetricsClient.getInstance());

        FilterWorker spy= spy(worker);
        doReturn(3L).when(spy).getPublisherId(4L);

        FilterMessage fm = spy.processMessage(lm);
        assertEquals(1, f.testCalled);
        assertEquals(3L, fm.getPublisherId().longValue());
        assertEquals(4L, fm.getCampaignId().longValue());
        assertEquals(1L, fm.getSnapshotId().longValue());
        assertEquals(true, fm.getValid());
        assertEquals(FilterRuleType.NONE.toString(), fm.getFilterFailed());
    }

    @Test
    public void testWorkerSurvivesFilterFailure() {
        MockFilterFail f = new MockFilterFail();
        ListenerMessage lm = new ListenerMessage();
        lm.setPublisherId(1L);
        lm.setCampaignId(5L);
        lm.setSnapshotId(3L);

        MockKafka k = new MockKafka();
        k.pollResult = new ArrayList<ListenerMessage>();
        FilterWorker worker = new FilterWorker(f, k, MetricsClient.getInstance());

        FilterWorker spy= spy(worker);
        doReturn(1L).when(spy).getPublisherId(5L);

        FilterMessage fm = spy.processMessage(lm);
        assertEquals(1L, fm.getPublisherId().longValue());
        assertEquals(5L, fm.getCampaignId().longValue());
        assertEquals(1, f.testCalled);
        assertEquals(3L, fm.getSnapshotId().longValue());
        assertEquals(false, fm.getValid());
        assertEquals(FilterRuleType.ERROR.toString(), fm.getFilterFailed());
    }

    /**
     * Wait for up to a second for a condition to become true
     * @param condition
     * @throws InterruptedException
     */
    private void helperTimeoutCheck(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 10; ++i) {
            if(condition.getAsBoolean()) {
                return;
            }

            Thread.sleep(100);
        }
        assertTrue(false);
    }
}

