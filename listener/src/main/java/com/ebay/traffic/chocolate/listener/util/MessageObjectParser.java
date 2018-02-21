package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Enumeration;

/**
 * Utility class for parsing the POJOs
 * 
 * @author kanliu
 */
public class MessageObjectParser {

    /** Logging instance */
    private static final Logger logger = Logger.getLogger(MessageObjectParser.class);
    private static MessageObjectParser INSTANCE;
    private static final long DEFAULT_PUBLISHER_ID = -1L;
    private static final String REDIRECT_SPECIAL_TAG = "chocolateSauce=";
    private static final String REDIRECT_HEADER = "Location";
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
            final LogicalChannelEnum logicalChannel, final ChannelActionEnum action, String snid) {

        ListenerMessage record = new ListenerMessage();
        // Set URI first as multipleRedirectHandler may overwrite URI
        record.setUri(this.getRequestURL(clientRequest));
        // Handle multiple redirect logic, rover use 301 for redirect
        // Set the channel type + HTTP headers + channel action

        record.setChannelType(logicalChannel.getAvro());
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

    /**
     * Get Request URL based on different cases
     * If Request URL contains REDIRECT_SPECIAL_TAG, use the tag value as request URL
     * else use the request URL from clientRequest
     * @param clientRequest Request
     * @return URL in String
     */
    protected String getRequestURL(HttpServletRequest clientRequest) {
        String query = clientRequest.getQueryString();
        String url = new ServletServerHttpRequest(clientRequest).getURI().toString();
        if(query != null && ! query.isEmpty()) {
            for (String k : Arrays.asList(query.split("&"))) {
                if (k.startsWith(REDIRECT_SPECIAL_TAG)) {
                    try {
                        url = URLDecoder.decode(k.split("=",2)[1],StandardCharsets.UTF_8.toString());
                        break;
                    } catch(UnsupportedEncodingException e){
                        logger.warn("Wrong encoding for request", e);
                    }
                }
            }
        }

        return url;
    }

    /**
     * Filter on Response Location header
     * @param clientRequest Request
     * @param proxyResponse Response, for first redirect to Rover, original request will
     *                      be attached to Location header as part of the URL query
     * @return filter result, true for filtered response
     */
    public boolean responseShouldBeFiltered(HttpServletRequest clientRequest, HttpServletResponse proxyResponse)
            throws MalformedURLException, UnsupportedEncodingException {
        // no action if no redirect
        if( proxyResponse.getStatus() != 301 || proxyResponse.getHeader(REDIRECT_HEADER) == null )
        {
            return false;
        }

        // assume Rover always return valid URL
        URL redirectUrl = new URL(proxyResponse.getHeader(REDIRECT_HEADER));

        // redirect to other domain
        if(!redirectUrl.getHost().startsWith("rover")) {
            return false;
        }

        // redirect to Rover case
        // no need to send message to Kafka
        // the first time redirect to Rover, append the request URL to query
        // seems Rover will not keep the REDIRECT_SPECIAL_TAG in Location header, need append again from the original request
        boolean withQuery = (redirectUrl.getQuery() != null);

        String requestUrlEncoded = URLEncoder.encode(this.getRequestURL(clientRequest), StandardCharsets.UTF_8.toString());
        if (withQuery) {
            proxyResponse.setHeader(REDIRECT_HEADER, redirectUrl.toString() + "&" + REDIRECT_SPECIAL_TAG + requestUrlEncoded);
        } else {
            proxyResponse.setHeader(REDIRECT_HEADER, redirectUrl.toString() + "?" + REDIRECT_SPECIAL_TAG + requestUrlEncoded);
        }
        return true;
    }



    private String serializeRequestHeaders(HttpServletRequest clientRequest) {
        StringBuilder requestHeaders = new StringBuilder();
        for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements();) {
            String headerName = e.nextElement();
            requestHeaders.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
        }
        requestHeaders.deleteCharAt(0);
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