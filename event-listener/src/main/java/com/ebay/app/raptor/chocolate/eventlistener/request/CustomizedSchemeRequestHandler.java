/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.request;

import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.traffic.monitoring.ESMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.deeplinkEbaySites;

/**
 * @author xiangli4
 * Handle customized scheme deeplink requests
 */
@Component
@DependsOn("EventListenerService")
public class CustomizedSchemeRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomizedSchemeRequestHandler.class);

  private static final String VIEWITEM = "item.view";

  public Event parseCustomizedSchemeEvent(String targetUrl, String referer) {

    UriComponents deeplinkUriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

    MultiValueMap<String, String> deeplinkParameters = deeplinkUriComponents.getQueryParams();

    if (deeplinkParameters.containsKey(MKEVT)
            && deeplinkParameters.containsKey(MKCID) && deeplinkParameters.containsKey(MKRID)){
      if (deeplinkParameters.get(NAV).get(0).equals(VIEWITEM) && !deeplinkParameters.get(ID).get(0).isEmpty()) {
        String viewItemChocolateURL = CollectionServiceUtil.constructViewItemChocolateURLForDeepLink(deeplinkParameters);
        if (!viewItemChocolateURL.isEmpty()) {
          ESMetrics.getInstance().meter("IncomingAppDeepLinkWithValidNativeURISuccess");
          Event event = constructDeeplinkEvent(viewItemChocolateURL, referer);
          return event;
        } else {
          LOGGER.warn(Errors.ERROR_INVALID_NATIVE_URI_DEEPLINK);
          ESMetrics.getInstance().meter(Errors.ERROR_INVALID_NATIVE_URI_DEEPLINK);
          return null;
        }
      }
    } else if (deeplinkParameters.containsKey(REFERRER)) {
      String deeplinkTargetUrl = deeplinkParameters.get(REFERRER).get(0);

      try {
        if(deeplinkTargetUrl.startsWith(HTTPS_ENCODED) || deeplinkTargetUrl.startsWith(HTTP_ENCODED)) {
          deeplinkTargetUrl = URLDecoder.decode(deeplinkTargetUrl, StandardCharsets.UTF_8.name());
        }
      } catch (Exception ex) {
        ESMetrics.getInstance().meter("DecodeDeepLinkTargetUrlError");
        LOGGER.warn("Decode deeplink target url error." + ex.getMessage());
      }

      Matcher deeplinkEbaySitesMatcher = deeplinkEbaySites.matcher(deeplinkTargetUrl.toLowerCase());
      if (deeplinkEbaySitesMatcher.find()) {
        ESMetrics.getInstance().meter("IncomingAppDeepLinkWithValidTargetURLSuccess");
        Event event = constructDeeplinkEvent(deeplinkTargetUrl, referer);
        return event;
      } else {
        LOGGER.warn(Errors.ERROR_INVALID_TARGET_URL_DEEPLINK);
        ESMetrics.getInstance().meter(Errors.ERROR_INVALID_TARGET_URL_DEEPLINK);
        return null;
      }
    } else {
      LOGGER.warn(Errors.ERROR_NO_VALID_TRACKING_PARAMS_DEEPLINK);
      return null;
    }

    return null;
  }

  private Event constructDeeplinkEvent (String targetUrl, String referer) {
    Event event = new Event();
    event.setTargetUrl(targetUrl);
    event.setReferrer(referer);
    return event;
  }
}
