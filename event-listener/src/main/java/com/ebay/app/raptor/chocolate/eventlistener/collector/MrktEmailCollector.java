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
import java.net.URLDecoder;

import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_ACTION;
import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_TYPE;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ebaysites;

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
   * @param parameters    url parameters
   * @param type          channel type
   * @param action        action type
   * @param request       http request
   * @param uri           url
   * @param channelAction channel action enum
   */
  public void trackUbi(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters, String type,
                       String action, HttpServletRequest request, String uri, String referer, String utpEventId,
                       ChannelAction channelAction) {
    // send click event to ubi
    // Third party clicks should not be tracked into ubi
    // Don't track ubi if the click is a duplicate itm click
    if (ChannelAction.CLICK.equals(channelAction) && ebaysites.matcher(uri.toLowerCase()).find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker =
            (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // common tags and soj tags
        super.trackUbi(requestContext, parameters, type, action, request, uri, referer, utpEventId, channelAction);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

        // fbprefetch
        if (isFacebookPrefetchEnabled(request))
          requestTracker.addTag("fbprefetch", true, Boolean.class);

        // channel id
        addTagFromUrlQuery(parameters, requestTracker, Constants.MKCID, "chnl", String.class);

        // source id
        addTagFromUrlQuery(parameters, requestTracker, Constants.SOURCE_ID, "emsid", String.class);

        // email id
        addTagFromUrlQuery(parameters, requestTracker, Constants.BEST_GUESS_USER, "emid", String.class);

        // campaign run date
        addTagFromUrlQuery(parameters, requestTracker, Constants.CAMP_RUN_DT, "crd", String.class);

        // segment name
        addTagFromUrlQuery(parameters, requestTracker, Constants.SEGMENT_NAME, "segname", String.class);

        // shortened segment name
        addTagFromUrlQuery(parameters, requestTracker, Constants.SEGMENT_NAME_S, "segname", String.class);

        // Yesmail message master id
        addTagFromUrlQuery(parameters, requestTracker, Constants.YM_MSSG_MSTR_ID, "ymmmid", String.class);

        // YesMail message id
        addTagFromUrlQuery(parameters, requestTracker, Constants.YM_MSSG_ID, "ymsid", String.class);

        // Yesmail mailing instance
        addTagFromUrlQuery(parameters, requestTracker, Constants.YM_INSTC, "yminstc", String.class);

        // decrypted user id
        addDecrytpedUserIDFromBu(parameters, requestTracker);

        // Adobe email redirect url
        if (parameters.containsKey(Constants.REDIRECT_URL_SOJ_TAG)
            && parameters.get(Constants.REDIRECT_URL_SOJ_TAG).get(0) != null) {
          requestTracker.addTag("adcamp_landingpage",
              URLDecoder.decode(parameters.get(Constants.REDIRECT_URL_SOJ_TAG).get(0), "UTF-8"), String.class);
        }

        // Adobe email redirect source
        addTagFromUrlQuery(parameters, requestTracker, Constants.REDIRECT_SRC_SOJ_SOURCE, "adcamp_locationsrc",
            String.class);

        //Adobe campaign public user id
        addTagFromUrlQuery(parameters, requestTracker, Constants.ADOBE_CAMP_PUBLIC_USER_ID, "adcamppu", String.class);

      } catch (Exception e) {
        LOGGER.warn("Error when tracking ubi for marketing email click tags", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
      }
    }
  }
}
