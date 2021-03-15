/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.monitoring.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_ACTION;
import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_TYPE;
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
   * @param parameters    url parameters
   * @param type          channel type
   * @param action        action type
   * @param request       http request
   * @param uri           url
   * @param channelAction channel action enum
   */
  @Override
  public void trackUbi(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                       String type, String action, HttpServletRequest request, String uri, String referer,
                       String utpEventId, ChannelAction channelAction) {
    // send click event to ubi
    // Third party clicks should not be tracked into ubi
    // Don't track ubi if the click is a duplicate itm click
    if (ChannelAction.CLICK.equals(channelAction) && ebaysites.matcher(uri.toLowerCase()).find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker =
            (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // soj tags
        super.trackUbi(requestContext, parameters, type, action, request, uri, referer, utpEventId, channelAction);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

        // fbprefetch
        if (isFacebookPrefetchEnabled(request)) {
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

      } catch (Exception e) {
        LOGGER.warn("Error when tracking ubi for site email click tags", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
      }
    }
  }
}
