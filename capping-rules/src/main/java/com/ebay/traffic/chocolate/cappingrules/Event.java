package com.ebay.traffic.chocolate.cappingrules;

import java.io.Serializable;

public class Event implements Serializable {
    public Long getSnapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(Long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getPublisherId() {
        return publisherId;
    }

    public void setPublisherId(Long publisherId) {
        this.publisherId = publisherId;
    }

    public Long getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(Long campaignId) {
        this.campaignId = campaignId;
    }

    public Long getSnid() {
        return snid;
    }

    public void setSnid(Long snid) {
        this.snid = snid;
    }

    public Boolean getTracked() {
        return isTracked;
    }

    public void setTracked(Boolean tracked) {
        isTracked = tracked;
    }

    public Boolean getValid() {
        return isValid;
    }

    public void setValid(Boolean valid) {
        isValid = valid;
    }

    private Long snapshotId;
    private Long timestamp;
    private Long publisherId;
    private Long campaignId;
    private Long snid;
    private Boolean isTracked;
    private Boolean isValid;

    public Event() {

    }

    public Event(long snapshotId, boolean isValid) {
        this.snapshotId = snapshotId;
        this.isValid = isValid;
    }

    public Event(long snapshotId, Long timestamp, long publisherId, long campaignId, long snid, boolean isTracked, boolean isValid) {
        this.snapshotId = snapshotId;
        this.timestamp = timestamp;
        this.publisherId = publisherId;
        this.campaignId = campaignId;
        this.snid = snid;
        this.isTracked = isTracked;
        this.isValid = true;
    }
}
