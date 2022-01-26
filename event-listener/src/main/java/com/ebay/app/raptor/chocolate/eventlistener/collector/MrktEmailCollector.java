/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.monitoring.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.deeplinksites;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ebaySitesIncludeULK;

/**
 * @author xiangli4
 * Track
 * 1. Ubi message,
 * 2. Behavior message
 * for marketing email
 */
@Component
@DependsOn("EventListenerService")
public class MrktEmailCollector extends CustomerMarketingCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MrktEmailCollector.class);

  @PostConstruct
  @Override
  public void postInit() {
    super.postInit();
  }

  /**
   * Track ubi
   *
   * @param requestContext request context
   * @param baseEvent      base event
   */
  @Override
  public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent) {
    // send click event to ubi
    // Third party clicks should not be tracked into ubi
    if (ChannelActionEnum.CLICK.equals(baseEvent.getActionType())
            && (ebaySitesIncludeULK.matcher(baseEvent.getUrl().toLowerCase()).find() || deeplinksites.matcher(baseEvent.getUrl().toLowerCase()).find())) {
      MultiValueMap<String, String> parameters = baseEvent.getUrlParameters();

      // Ubi tracking
      IRequestScopeTracker requestTracker =
              (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      // common tags and soj tags
      super.trackUbi(requestContext, baseEvent);

      //for debugging
      requestTracker.addTag("nonSojTag", 1, Integer.class);

      addUbiTag(mktEmailParamTags, baseEvent, requestTracker);
      // fbprefetch
      if (CollectionServiceUtil.isFacebookPrefetchEnabled(baseEvent.getRequestHeaders())) {
        requestTracker.addTag("fbprefetch", true, Boolean.class);
      }
      // decrypted user id
      addDecrytpedUserIDFromBu(parameters, requestTracker);
      // Adobe email redirect url
      try {
        if (parameters.containsKey(REDIRECT_URL_SOJ_TAG)
                && parameters.get(REDIRECT_URL_SOJ_TAG).get(0) != null) {
          requestTracker.addTag("adcamp_landingpage",
                  URLDecoder.decode(parameters.get(REDIRECT_URL_SOJ_TAG).get(0), "UTF-8"), String.class);
        }
      } catch (UnsupportedEncodingException e) {
        LOGGER.warn("adcamp_landingpage is wrongly encoded", e);
        MonitorUtil.info("ErrorEncoded_adcamp_landingpage", 1, Field.of(CHANNEL_ACTION, baseEvent.getActionType()),
                Field.of(CHANNEL_TYPE, baseEvent.getChannelType()));
      }
    }
  }
}
