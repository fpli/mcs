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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
    public void testMessage() throws IOException {
        Long startTime = 1L;
        Long campaignId = 2L;
        Long wrongCampaingId = 3L;
        LogicalChannelEnum logicalChannel = LogicalChannelEnum.EPN;
        ChannelActionEnum action = ChannelActionEnum.CLICK;
        mockClientRequest.setMethod("GET");
        mockClientRequest.addHeader("Some", "Header");
        mockProxyResponse.setHeader("SomeMore", "Headers");

        mockProxyResponse.setStatus(200);
        mockProxyResponse.setStatus(301);
        mockClientRequest.setScheme("http");
        mockClientRequest.setServerName("rover.ebay.com");
        mockClientRequest.setRequestURI("/rover/1/707-53477-19255-0/1?vectorid=229487&lgeo=1&toolid=10039&item=132289807354&raptor=1&ff3=2&campid=5336987918&mpre=http%3A%2F%2Fwww.ebay.de%2Fitm%2Flike%2F132289807354%3Fclk_rvr_id%3D1588198933946%26lgeo%3D1%26vectorid%3D229487%26item%3D132289807354%26raptor%3D1%26rmvSB%3Dtrue&cguid=5f0effd91640ac3d2a575c0cfd01b63d&rvrrefts=69ab37ce1640ad4cfcf08d5ffff8f331&chocolateSauce=http%3A%2F%2Frover.ebay.com%2Frover%2F1%2F707-53477-19255-0%2F1%3Fff3%3D2%26toolid%3D10039%26campid%3D5336987918%26item%3D132289807354%26vectorid%3D229487%26lgeo%3D1%26raptor%3D1");
        mockClientRequest.setServerPort(80);


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
}

