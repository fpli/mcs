package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;

import java.net.MalformedURLException;
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

        for (Map.Entry<String, String> item : requestHeaders.entrySet()) {
            String key = item.getKey();
            if (key.equalsIgnoreCase("X-Purpose") && item.getValue().equalsIgnoreCase("preview") ||
                    key.equalsIgnoreCase("X-Moz") && item.getValue().equalsIgnoreCase("prefetch")) {
                this.isPrefetch = true;
            } else if (key.equalsIgnoreCase("Cookie")) {
                this.requestCguid = parseCguidFromCookie(item.getValue());
                this.requestCguidTimestamp = parseTimestampFromCguid(this.requestCguid);
            }
        }

        this.referrerDomain = getDomainName(message.getReferer());
        this.sourceIP = message.getRemoteIp();
        this.userAgent = message.getUserAgent();
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

    public String getRequestCguid() {
        return this.requestCguid;
    }

    public void setRequestCguid(String cguid) {
        this.requestCguid = cguid;
    }

    public long getRequestCguidTimestamp() {
        return this.requestCguidTimestamp;
    }

    public void setRequestCguidTimestamp(long cguidTimestamp) {
        this.requestCguidTimestamp = cguidTimestamp;
    }

    public long getCampaignId() {
        return this.campaignId;
    }

    public void setCampaignId(long campaignId) {
        this.campaignId = campaignId;
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
        if (url == null || url.isEmpty()) {
            return url;
        }

        try {
            String fullUrl = (url.toLowerCase().startsWith("http://") || url.toLowerCase().startsWith("https://")) ? url : "http://" + url;
            URL uri = new URL(fullUrl);
            String domain = uri.getHost();
            return domain.startsWith("www.") ? domain.substring(4) : domain;
        } catch (MalformedURLException e) {
            return "";
        }
    }

    private String parseCguidFromCookie(String cookieStr) {
        int cguidPos = cookieStr.indexOf("cguid/");
        if (cguidPos < 0) {
            return null;
        }
        return cookieStr.substring(cguidPos + 6, cguidPos + 38);
    }

    private long parseTimestampFromCguid(String cguid) {
        if (cguid == null) {
            return 0;
        }

        String milliStr = cguid.substring(8, 11) + cguid.substring(0, 8);
        return Long.parseLong(milliStr, 16);
    }
}
