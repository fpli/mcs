package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class DefaultChannelTest {
    private DefaultChannel handler;
    private MetricsClient metrics;
    private MockHttpServletRequest request;

    @Before
    public void setUp() throws Exception{
        metrics = mock(MetricsClient.class);
        handler = new DefaultChannel(metrics);

        request = new MockHttpServletRequest();
        request.setParameter("foo", "bar");
    }
    
    @Test
    public void testProcessShouldLogUnManagedMetric() throws InterruptedException, ExecutionException, IOException {
        handler.process(request, mock(HttpServletResponse.class));
        verify(metrics, times(1)).meter(eq("un-managed"));
    }

    @Test
    public void testPartitionKey() {
        Long partitionKey = handler.getPartitionKey(request);
        assertEquals(Long.valueOf(request.hashCode()), partitionKey);
    }
}
