package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * FilterContainer-internal representation of the filterable event.
 * Constructed from ListenerMessage.
 * <p>
 * Created by spugach on 11/15/16.
 */
public class FilterRequest {
    private ChannelAction channelAction;
    private String userAgent;
    private String sourceIP = "";
    private String referrerDomain = "";
    private boolean isPrefetch = false;
    private long timestamp = 0;
    private String requestCguid = null;
    private long requestCguidTimestamp;
    private String responseCguid = null;
    private long responseCguidTimestamp;
    private String rotationId;
    private long campaignId = 0;
    private long publisherId = 0;
    private HttpMethod protocol = null;

    /**
     * Default constructor for testing
     */
    public FilterRequest() {

    }

    /**
     * Construct a FilterRequest from a ListenerMessage by parsing out from the request headers
     *
     * @param message Source ListenerMessage
     */
    public FilterRequest(ListenerMessage message) {
        HashMap<String, String> requestHeaders = processHeaders(message.getRequestHeaders());

        this.parseUri(message.getUri());

        for (Map.Entry<String, String> item : requestHeaders.entrySet()) {
            String key = item.getKey();
            if (key.equalsIgnoreCase("Referer")) {
                this.referrerDomain = getDomainName(item.getValue());
            } else if (key.equalsIgnoreCase("X-Purpose") && item.getValue().equalsIgnoreCase("preview") ||
                    key.equalsIgnoreCase("X-Moz") && item.getValue().equalsIgnoreCase("prefetch")) {
                this.isPrefetch = true;
            } else if (key.equalsIgnoreCase("X-EBAY-CLIENT-IP")) {      // This header takes precedence
                this.sourceIP = item.getValue();
            } else if (key.equalsIgnoreCase("X-Forwarded-For") && (this.sourceIP == null || this.sourceIP.isEmpty())) {  // This header works only if there is no X-EBAY-CLIENT-IP
                this.sourceIP = item.getValue();
            } else if (key.equalsIgnoreCase("User-Agent")) {
                this.userAgent = item.getValue();
            } else if (key.equalsIgnoreCase("Cookie")) {
                this.requestCguid = parseCGUIDFromCookie(item.getValue());
                this.requestCguidTimestamp = parseTimestampFromCGUID(this.requestCguid);
            }
        }

        HashMap<String, String> responseHeaders = processHeaders(message.getResponseHeaders());
        for (Map.Entry<String, String> item : responseHeaders.entrySet()) {
            String key = item.getKey();
            if (key.equalsIgnoreCase("Set-Cookie")) {
                this.responseCguid = parseCGUIDFromCookie(item.getValue());
                this.responseCguidTimestamp = parseTimestampFromCGUID(this.responseCguid);
            }
        }

        this.timestamp = message.getTimestamp();
        this.publisherId = message.getPublisherId();
        this.campaignId = message.getCampaignId();
        this.channelAction = message.getChannelAction();
        this.protocol = message.getHttpMethod();
    }

    //
    // Getters and setters
    //

    public String getUserAgent() {
        return this.userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getSourceIP() {
        return this.sourceIP;
    }

    public void setSourceIP(String sourceIP) {
        this.sourceIP = sourceIP;
    }

    public String getReferrerDomain() {
        return this.referrerDomain;
    }

    public void setReferrerDomain(String referrerDomain) {
        this.referrerDomain = referrerDomain;
    }

    public boolean isPrefetch() {
        return this.isPrefetch;
    }

    public void setPrefetch(boolean prefetch) {
        this.isPrefetch = prefetch;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ChannelAction getChannelAction() {
        return this.channelAction;
    }

    public void setChannelAction(ChannelAction action) {
        this.channelAction = action;
    }

    public String getRequestCGUID() {
        return this.requestCguid;
    }

    public void setRequestCGUID(String cguid) {
        this.requestCguid = cguid;
    }

    public long getRequestCGUIDTimestamp() {
        return this.requestCguidTimestamp;
    }

    public void setRequestCGUIDTimestamp(long cguidTimestamp) {
        this.requestCguidTimestamp = cguidTimestamp;
    }

    public String getResponseCGUID() {
        return this.responseCguid;
    }

    public void setResponseCGUID(String cguid) {
        this.responseCguid = cguid;
    }

    public long getResponseCGUIDTimestamp() {
        return this.responseCguidTimestamp;
    }

    public void setResponseCGUIDTimestamp(long cguidTimestamp) {
        this.responseCguidTimestamp = cguidTimestamp;
    }

    public long getCampaignId() {
        return this.campaignId;
    }

    public void setCampaignId(long campaignId) {
        this.campaignId = campaignId;
    }

    public String getRotationID() {
        return this.rotationId;
    }

    public void setRotationId(String rotationId) {
        this.rotationId = rotationId;
    }

    public long getPublisherId() {
        return this.publisherId;
    }

    public void setPublisherId(long publisherId) {
        this.publisherId = publisherId;
    }

    public HttpMethod getProtocol() {
        return this.protocol;
    }

    public void setProtocol(HttpMethod protocol){
        this.protocol = protocol;
    }

    /**
     * Parse a request-headers string to a key-value form
     *
     * @param headerLine request headers
     * @return key-velue hashmap of  headers
     */
    private static HashMap<String, String> processHeaders(String headerLine) {
        HashMap<String, String> result = new HashMap<>();
        if (null == headerLine) {
            return result;
        }

        String[] parts = headerLine.split("\\|");
        for (String part : parts) {
            int splitter = part.indexOf(':');
            if (splitter <= 0 || splitter + 1 >= part.length()) {
                continue;
            }
            result.put(part.substring(0, splitter).trim(), part.substring(splitter + 1).trim());
        }

        return result;
    }

    /**
     * Crop an URL to get just the domain name
     *
     * @param url full URL
     * @return cropped domain name
     */
    private static String getDomainName(String url) {
        try {
            String fullUrl = (url.toLowerCase().startsWith("http://") || url.toLowerCase().startsWith("https://")) ? url : "http://" + url;
            URL uri = new URL(fullUrl);
            String domain = uri.getHost();
            return domain.startsWith("www.") ? domain.substring(4) : domain;
        } catch (MalformedURLException e) {
            return "";
        }
    }

    /**
     * Extract rotation ID and campaign ID from the URI
     *
     * @param uri URI
     */
    private void parseUri(String uri) {
        if (uri == null || uri.isEmpty()) {
            Logger.getLogger(FilterRequest.class).error("Empty URI in request/response");
            throw new RuntimeException("Empty URI");
        }

        try {
            URI url = new URI(uri);
            String path = url.getPath();
            String[] pathBits = path.split("/");
            if (pathBits.length >= 4 && url.getHost().startsWith("rover")) {
                this.rotationId = pathBits[3];
            } else if (pathBits.length >= 3) {
                this.rotationId = pathBits[2];
            }
        } catch (Exception e) {
            Logger.getLogger(FilterRequest.class).error("Malformed URI in request/response");
            throw new RuntimeException("Unable to parse URI: " + StringUtils.abbreviate(uri, 100), e);
        }
    }

    private String parseCGUIDFromCookie(String cookieStr) {
        int cguidPos = cookieStr.indexOf("cguid/");
        if (cguidPos < 0) {
            return null;
        }
        return cookieStr.substring(cguidPos + 6, cguidPos + 38);
    }

    private long parseTimestampFromCGUID(String cguid) {
        if (cguid == null) {
            return 0;
        }

        String milliStr = cguid.substring(8, 11) + cguid.substring(0, 8);
        return Long.parseLong(milliStr, 16);
    }
}
