package com.ebay.traffic.chocolate.cappingrules;

import java.io.Serializable;

public class Event implements Serializable {
    public long getSnapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getPublisherId() {
        return publisherId;
    }

    public void setPublisherId(long publisherId) {
        this.publisherId = publisherId;
    }

    public long getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(long campaignId) {
        this.campaignId = campaignId;
    }

    public long getSnid() {
        return snid;
    }

    public void setSnid(long snid) {
        this.snid = snid;
    }

    public String getRequestHeaders() {
        return requestHeaders;
    }

    public void setRequestHeaders(String requestHeaders) {
        this.requestHeaders = requestHeaders;
    }

    public boolean getTracked() {
        return isTracked;
    }

    public void setTracked(boolean tracked) {
        isTracked = tracked;
    }

    public boolean getValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    private long snapshotId;
    private long timestamp;
    private long publisherId;
    private long campaignId;
    private long snid;
    private String requestHeaders;
    private boolean isTracked;
    private boolean isValid;

    public Event() {

    }

    public Event(long snapshotId, boolean isValid) {
        this.snapshotId = snapshotId;
        this.isValid = isValid;
    }

    public Event(long snapshotId, long timestamp, long publisherId, long campaignId, long snid, String requestHeaders, boolean isTracked, boolean isValid) {
        this.snapshotId = snapshotId;
        this.timestamp = timestamp;
        this.publisherId = publisherId;
        this.campaignId = campaignId;
        this.snid = snid;
        this.requestHeaders = requestHeaders;
        this.isTracked = isTracked;
        this.isValid = true;
    }
}
