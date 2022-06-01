/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.tracking.api.IRequestScopeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.ws.rs.container.ContainerRequestContext;

import static com.ebay.app.raptor.chocolate.constant.Constants.MKCID;
import static com.ebay.app.raptor.chocolate.constant.Constants.SMS_ID;
import static com.ebay.app.raptor.chocolate.constant.Constants.TAG_CHANNEL;

@Component
@DependsOn("EventListenerService")
public class SMSCollector extends CustomerMarketingCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SiteEmailCollector.class);

  @PostConstruct
  @Override
  public void postInit() {
    super.postInit();
  }

  @Override
  public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent) {

    IRequestScopeTracker requestTracker =
        (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

    // common tags and soj tags
    super.trackUbi(requestContext, baseEvent);

    // channel id
    addTagFromUrlQuery(baseEvent.getUrlParameters(), requestTracker, MKCID, TAG_CHANNEL, String.class);

    // sms unique id
    addTagFromUrlQuery(baseEvent.getUrlParameters(), requestTracker, SMS_ID, "smsid", String.class);
  }
}
