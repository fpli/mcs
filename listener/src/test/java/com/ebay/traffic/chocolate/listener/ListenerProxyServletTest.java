package com.ebay.traffic.chocolate.listener;

import org.eclipse.jetty.server.Request;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class ListenerProxyServletTest {

    @Test
    public void reencodeQueryShouldEncodeBadQueryStrings() {
        Request request = mock(Request.class);
        when(request.getQueryString()).thenReturn("campid=^");

        ListenerProxyServlet servlet = new ListenerProxyServlet();
        servlet.reencodeQuery(request);
        verify(request, times(1)).setQueryString("campid=%5E");
    }
}
