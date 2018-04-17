package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by kanliu on 7/13/2017.
 */
public class MessageObjectParserTest {
    private static MessageObjectParser parser;
    private MockHttpServletRequest mockClientRequest;
    private MockHttpServletResponse mockProxyResponse;


    @Before
    public void setUp() throws Exception {
        MessageObjectParser.init();
        parser = MessageObjectParser.getInstance();
        mockClientRequest = new MockHttpServletRequest();
        mockProxyResponse = new MockHttpServletResponse();
        List<Pair<Long, Long>> records = new ArrayList<>();
        Long campaignId = 2L;
        Long publisherId = 111L;

        records.add(Pair.of(campaignId, publisherId));
    }

    @Test
    public void testResponseFilter() throws MalformedURLException, UnsupportedEncodingException {
        mockProxyResponse.setStatus(200);
        assertFalse(parser.responseShouldBeFiltered(mockClientRequest, mockProxyResponse));
        mockProxyResponse.setStatus(301);
        assertFalse(parser.responseShouldBeFiltered(mockClientRequest, mockProxyResponse));
        mockProxyResponse.setHeader("location", "http://www.ebay.com/itm/12345");
        assertFalse(parser.responseShouldBeFiltered(mockClientRequest, mockProxyResponse));
        mockProxyResponse.setHeader("location", "https://rover.ebay.co.uk/1/2/9?a=b");
        mockClientRequest.setScheme("http");
        mockClientRequest.setServerName("rover.ebay.com");
        mockClientRequest.setRequestURI("/a/b/c");
        mockClientRequest.setServerPort(80);

        assertTrue(parser.responseShouldBeFiltered(mockClientRequest, mockProxyResponse));
        assertEquals("https://rover.ebay.co.uk/1/2/9?a=b&chocolateSauce=http%3A%2F%2Frover.ebay.com%2Fa%2Fb%2Fc", mockProxyResponse.getHeader("Location"));

        mockProxyResponse.setHeader("location", "https://rover.ebay.de/1/2/9");
        assertTrue(parser.responseShouldBeFiltered(mockClientRequest, mockProxyResponse));
        assertEquals("https://rover.ebay.de/1/2/9?chocolateSauce=http%3A%2F%2Frover.ebay.com%2Fa%2Fb%2Fc", mockProxyResponse.getHeader("Location"));

        mockProxyResponse.setHeader("Location", "https://rover.ebay.de/1/2/9?chocolateSauce=http%3A%2F%2Frover.ebay.com%2Fa%2Fb%2Fc");
        assertTrue(parser.responseShouldBeFiltered(mockClientRequest, mockProxyResponse));
    }

    @Test
    public void testMessage() throws IOException {
        Long startTime = 1L;
        Long campaignId = 2L;
        Long wrongCampaingId = 3L;
        LogicalChannelEnum logicalChannel = LogicalChannelEnum.EPN;
        ChannelActionEnum action = ChannelActionEnum.CLICK;
        mockClientRequest.setMethod("GET");
        mockClientRequest.addHeader("Some", "Header");
        mockProxyResponse.setHeader("SomeMore", "Headers");

        ListenerMessage record = parser.parseHeader(mockClientRequest, mockProxyResponse, startTime, campaignId, logicalChannel.getAvro(), action, "foo");

        assertEquals("Some: Header", record.getRequestHeaders());
        assertEquals("SomeMore: Headers", record.getResponseHeaders());
        assertEquals(Long.valueOf(-1L), record.getPublisherId());
        assertEquals(Long.valueOf(startTime), record.getTimestamp());
        assertEquals(action.getAvro(), record.getChannelAction());
        assertEquals(logicalChannel.getAvro(), record.getChannelType());
        assertEquals(HttpMethodEnum.parse("GET").getAvro(), record.getHttpMethod());
        assertEquals("foo", record.getSnid());

        mockClientRequest.setScheme("http");
        mockClientRequest.setServerName("rover.ebay.com");
        mockClientRequest.setRequestURI("/a/b/c");
        mockClientRequest.setServerPort(80);
        mockClientRequest.addHeader("a", "b");
        mockProxyResponse.setStatus(301);
        mockProxyResponse.setHeader("Location", "https://www.ebay.co.uk/1/2/9?a=b&chocolateSauce=http%3A%2F%2Frover.ebay.com%2Fa%2Fb%2Fc");
        record = parser.parseHeader(mockClientRequest, mockProxyResponse, startTime, wrongCampaingId, logicalChannel.getAvro(), action, null);

        assertEquals(wrongCampaingId, record.getCampaignId());
        assertEquals("http://rover.ebay.com/a/b/c", record.getUri());
        assertEquals("Some: Header|a: b", record.getRequestHeaders());
        assertEquals(Long.valueOf(-1L), record.getPublisherId());
        assertEquals("", record.getSnid());
        String result = record.writeToJSON();
        System.out.println(result);
        assertFalse(result.isEmpty());
        assertTrue(result.startsWith("{"));
        assertTrue(result.endsWith("}"));
    }

    @Test
    public void testGetRequestURL() throws MalformedURLException, UnsupportedEncodingException {
        mockClientRequest.setScheme("http");
        mockClientRequest.setServerName("rover.ebay.com");
        mockClientRequest.setRequestURI("/a/b/c");
        mockClientRequest.setServerPort(80);
        mockClientRequest.addHeader("a", "b");
        mockProxyResponse.setStatus(301);
        mockProxyResponse.setHeader("Location", "https://www.ebay.co.uk/1/2/9?a=b&chocolateSauce=http%3A%2F%2Frover.ebay.com%2Fa%2Fb%2Fc");

        String url = parser.getRequestURL(mockClientRequest);
        assertEquals("http://rover.ebay.com/a/b/c", url);
    }
}

