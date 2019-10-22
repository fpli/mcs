package com.ebay.app.raptor.chocolate.adservice.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;


@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketingTrackingEvent implements Serializable {

	@JsonProperty("targetUrl")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String targetUrl = null;

	public String getTargetUrl() {
		return targetUrl;
	}

	public void setTargetUrl(String targetUrl) {
		this.targetUrl = targetUrl;
	}
}
