/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.request;

import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.util.HttpRequestUtil;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ePageSites;

/**
 * @author xiangli4
 * Handle static page specifically
 */
@Component
@DependsOn("EventListenerService")
public class StaticPageRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(StaticPageRequestHandler.class);


  public Event parseStaticPageEvent(String targetUrl, String referer) throws Exception {
    // For e page, the real target url is in the referer
    // Since Chrome strict policy, referer may be cut off, so use 'originalUrl' parameter first as target url
    // if referer is existed, it will be in the target url (request body) parameter
    Event event = new Event();
    String originalReferer = "";
    String targetPath = "";
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
    uriComponents.getQueryParams();
    originalReferer = uriComponents.getQueryParams().getFirst(Constants.EPAGE_REFERER);
    targetPath = uriComponents.getQueryParams().getFirst(Constants.EPAGE_URL);

    if (!StringUtils.isEmpty(targetPath)) {
      URIBuilder uriBuilder = new URIBuilder(URLDecoder.decode(targetPath, StandardCharsets.UTF_8.name()));
      uriBuilder.addParameters(new URIBuilder(targetUrl).getQueryParams());
      targetUrl = HttpRequestUtil.removeParam(uriBuilder.build().toString(), Constants.EPAGE_URL);
    } else {
      targetUrl = referer;
    }

    if (!StringUtils.isEmpty(originalReferer)) {
      referer = URLDecoder.decode(originalReferer, StandardCharsets.UTF_8.name());
      targetUrl = HttpRequestUtil.removeParam(targetUrl, Constants.EPAGE_REFERER);
    } else {
      LOGGER.warn(Errors.ERROR_NO_REFERER);
      ESMetrics.getInstance().meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }
    event.setTargetUrl(targetUrl);
    event.setReferrer(referer);
    return event;
  }
}
