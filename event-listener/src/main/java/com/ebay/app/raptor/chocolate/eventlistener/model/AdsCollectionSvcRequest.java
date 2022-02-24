package com.ebay.app.raptor.chocolate.eventlistener.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AdsCollectionSvcRequest {
    private String amdata = null;
    private String referrer = null;
    private String requestUrl = null;

    public String getAmdata() {
        return amdata;
    }

    public void setAmdata(String amdata) {
        this.amdata = amdata;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public String getRequestUrl() {
        return requestUrl;
    }

    public void setRequestUrl(String requestUrl) {
        this.requestUrl = requestUrl;
    }
}

