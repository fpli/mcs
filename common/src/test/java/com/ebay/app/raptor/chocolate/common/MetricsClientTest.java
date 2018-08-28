package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.ebayopensource.sherlock.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static javafx.scene.input.KeyCode.K;
import static javafx.scene.input.KeyCode.V;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("javadoc")
public class MetricsClientTest {

    /**
     * Frontier resolution
     */
    private static final int RESOLUTION = 60;

    /**
     * Frontier profile name
     */
    private static final String PROFILE_NAME = "Chocolate";

    @Before
    public void setUp() {
        assertNull(MetricsClient.INSTANCE);
    }

    @After
    public void shutDown() {
        MetricsClient.INSTANCE = null;
    }

    @Test
    public void testCtor() {
        MetricsClient client = new MetricsClient();
        assertNotNull(client.timer);
        assertEquals(PROFILE_NAME, client.profileName);
        assertEquals(RESOLUTION, client.frontierResolution);
        assertNull(client.session);
        assertTrue(client.meterMetrics.isEmpty());
        assertTrue(client.meanMetrics.isEmpty());
        assertEquals(client.sendCount, 0);
        client.terminate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullSession() {
        MetricsClient.init(null);
    }

    /**
     * agh, they don't allow exposure of the value-type which would make testing easier
     */
    private void testExpectedMetrics(Map<String, Metric> expected, String key) {
        assertTrue(expected.containsKey(key));
        Metric metric = expected.get(key);
        assertEquals(key, metric.name());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMeterBadName() {
        MetricsClient client = new MetricsClient();
        client.terminate();
        client.meter(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMeterEmptyName() {
        MetricsClient client = new MetricsClient();
        client.terminate();
        client.meter("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMeanBadName() {
        MetricsClient client = new MetricsClient();
        client.terminate();
        client.mean(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMeanEmptyName() {
        MetricsClient client = new MetricsClient();
        client.terminate();
        client.mean("", 1);
    }


    @Test
    public void testMeter() throws InterruptedException, ExecutionException {
        MetricsClient client = new MetricsClient();
        client.terminate();
        client.appSvcName =  "test";

//        assertNull(client.buildMetrics());

        client.meter("Once");
        client.meter("Once again", 1L);
        client.meter("Two", 2L);
        client.meter("Additive", 2L);
        client.meter("Additive", 4L);
        client.meter("Additive");
        client.meter("Additive");

        client.mean("meanzero", 0L);
        client.mean("meanavg", 3L);
        client.mean("meanavg", 4L);

        assertEquals(4, client.meterMetrics.size());
        assertEquals(1L, client.meterMetrics.get("Once").longValue());
        assertEquals(1L, client.meterMetrics.get("Once again").longValue());
        assertEquals(2L, client.meterMetrics.get("Two").longValue());
        assertEquals(8L, client.meterMetrics.get("Additive").longValue());

        assertEquals(2L, client.meanMetrics.size());
        assertEquals(0L, client.meanMetrics.get("meanzero").getLeft().longValue());
        assertEquals(7L, client.meanMetrics.get("meanavg").getLeft().longValue());

        assertEquals(1L, client.meanMetrics.get("meanzero").getRight().longValue());
        assertEquals(2L, client.meanMetrics.get("meanavg").getRight().longValue());

        MetricSet set = client.buildMetrics();
        client.meterMetrics.forEach((k,v) -> assertEquals(Long.valueOf(0L), v));
        client.meanMetrics.forEach((k,v) -> {
            assertEquals(Long.valueOf(0L), v.getLeft());
            assertEquals(Long.valueOf(0L), v.getRight());
        });

        assertEquals(RESOLUTION, set.resolutionSeconds());
        Map<String, Metric> metricsMap = set.metrics();
        testExpectedMetrics(metricsMap, "Once");
        testExpectedMetrics(metricsMap, "Once again");
        testExpectedMetrics(metricsMap, "Two");
        testExpectedMetrics(metricsMap, "Additive");
        testExpectedMetrics(metricsMap, "meanzero");
        testExpectedMetrics(metricsMap, "meanavg");

        // Now test sending. 
        client.session = mock(Session.class);
        Post mockPost = mock(Post.class);
        @SuppressWarnings("unchecked")
        ResultFuture<PostResult> mockFuture = mock(ResultFuture.class);

        when(client.session.post(set)).thenReturn(mockPost);
        when(mockPost.execute()).thenReturn(mockFuture);

        // Make sure nothing crashes on null
        client.send(null);
        verify(client.session, never()).post(set);
        verify(mockPost, never()).execute();
        verify(mockFuture, never()).get();

        assertEquals(0, client.getSendCount());
        client.send(set);
        assertEquals(1, client.getSendCount());
        verify(client.session, atLeastOnce()).post(set);
        verify(mockPost, atLeastOnce()).execute();
        verify(mockFuture, atLeastOnce()).get();
        verify(client.session, never()).close();
        client.send(set);
        assertEquals(2, client.getSendCount());

        // terminate.
        client.terminate();
        verify(client.session, atLeastOnce()).close();
    }

    @Test
    public void testInit() {
        Session mockSession = mock(Session.class);
        MetricsClient.init(mockSession);
        MetricsClient.INSTANCE.terminate(); // terminate immediately

        assertEquals(PROFILE_NAME, MetricsClient.profileName);
        assertEquals(RESOLUTION, MetricsClient.frontierResolution);
    }

}
