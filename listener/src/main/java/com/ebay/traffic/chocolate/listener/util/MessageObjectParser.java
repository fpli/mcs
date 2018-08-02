package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.kernel.util.StringUtils;
import com.ebay.traffic.chocolate.listener.ListenerProxyServlet;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing the POJOs
 * 
 * @author kanliu
 */
public class MessageObjectParser {

    private static MessageObjectParser INSTANCE;
    private static final long DEFAULT_PUBLISHER_ID = -1L;
    /* singleton class */
    private MessageObjectParser() {
    }
    
    /**
     * Convert a HTTP request to a listener message for Kafka.
     * 
     * @param clientRequest
     *            to use in parsing uri and timestamp
     * @param proxyResponse
     *            to use in parsing response headers
     * @param startTime
     *            as start time of the request
     * @return ListenerMessage  as the parse result.
     */
    public ListenerMessage parseHeader(
        final HttpServletRequest clientRequest,
        final HttpServletResponse proxyResponse, Long startTime, Long campaignId,
        final ChannelType channelType, final ChannelActionEnum action, String snid) {

        ListenerMessage record = new ListenerMessage();
        record.setUri(new ServletServerHttpRequest(clientRequest).getURI().toString());

        // Set the channel type + HTTP headers + channel action
        record.setChannelType(channelType);
        record.setHttpMethod(this.getMethod(clientRequest).getAvro());
        record.setChannelAction(action.getAvro());
        // Format record
        record.setRequestHeaders(serializeRequestHeaders(clientRequest));
        record.setResponseHeaders(serializeResponseHeaders(proxyResponse));
        record.setTimestamp(startTime);

        // Get snapshotId from request
        Long snapshotId = SnapshotId.getNext(ListenerOptions.getInstance().getDriverId(), startTime).getRepresentation();
        record.setSnapshotId(snapshotId);

        record.setCampaignId(campaignId);
        record.setPublisherId(DEFAULT_PUBLISHER_ID);
        record.setSnid((snid != null) ? snid : "");
        record.setIsTracked(false);     //TODO No messages are Durability-tracked for now

        return record;
    }

    /**
     * Get request method and validate from request
     * @param clientRequest Request
     * @return HttpMethodEnum
     */
    protected HttpMethodEnum getMethod(HttpServletRequest clientRequest) {
        HttpMethodEnum httpMethod = HttpMethodEnum.parse(clientRequest.getMethod());
        Validate.notNull(httpMethod, "Could not parse HTTP method from HTTP request=" + clientRequest.getMethod());
        return httpMethod;
    }

    private String serializeRequestHeaders(HttpServletRequest clientRequest) {
        StringBuilder requestHeaders = new StringBuilder();
        for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements();) {
            String headerName = e.nextElement();
            requestHeaders.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
        }
        if(!StringUtils.isEmpty(requestHeaders.toString())) requestHeaders.deleteCharAt(0);
        return requestHeaders.toString();
    }

    private String serializeResponseHeaders(HttpServletResponse response) {
        StringBuilder requestHeaders = new StringBuilder();
        for (String headerName: response.getHeaderNames()) {
            requestHeaders.append("|").append(headerName).append(": ").append(response.getHeader(headerName));
        }
        requestHeaders.deleteCharAt(0);
        return requestHeaders.toString();
    }

    /** returns the singleton instance */
    public static MessageObjectParser getInstance() {
        return INSTANCE;
    }

    /**
     * Initialize singleton instance
     */
    public static synchronized void init() {
        if (INSTANCE == null) {
            INSTANCE = new MessageObjectParser();
        }
    }
}