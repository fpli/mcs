/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import javax.annotation.PostConstruct;
import javax.ws.rs.container.ContainerRequestContext;

import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ebaysites;

/**
 * @author xiangli4
 * Track
 * 1. Ubi message,
 * 2. Behavior message
 * for site email
 */
@Component
@DependsOn("EventListenerService")
public class SiteEmailCollector extends CustomerMarketingCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SiteEmailCollector.class);

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
    // Don't track ubi if the click is a duplicate itm click
    if (ChannelActionEnum.CLICK.equals(baseEvent.getActionType())
        && ebaysites.matcher(baseEvent.getUrl().toLowerCase()).find()) {

      MultiValueMap<String, String> parameters = baseEvent.getUrlParameters();

      // Ubi tracking
      IRequestScopeTracker requestTracker =
          (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      // common tags and soj tags
      super.trackUbi(requestContext, baseEvent);

      // event family
      requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

      // fbprefetch
      if (isFacebookPrefetchEnabled(baseEvent.getRequestHeaders())) {
        requestTracker.addTag("fbprefetch", true, Boolean.class);
      }

      // channel id
      addTagFromUrlQuery(parameters, requestTracker, Constants.MKCID, "chnl", String.class);

      // source id
      addTagFromUrlQuery(parameters, requestTracker, Constants.SOURCE_ID, "emsid", String.class);

      // email unique id
      addTagFromUrlQuery(parameters, requestTracker, Constants.EMAIL_UNIQUE_ID, "euid", String.class);

      // email experienced treatment
      addTagFromUrlQuery(parameters, requestTracker, Constants.EXPRCD_TRTMT, "ext", String.class);

      // decrypted user id
      addDecrytpedUserIDFromBu(parameters, requestTracker);
    }
  }
}
