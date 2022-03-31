/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.request;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.traffic.monitoring.Field;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.ListUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
  private static final String CLICKEVENTFLAG = "1";

  private static final ImmutableList<String> smartBannerRids = ImmutableList.of("711-58542-18990-20",
          "711-58542-18990-20-1",
          "711-58542-18990-20-2",
          "711-58542-18990-20-3",
          "711-58542-18990-19",
          "711-58542-18990-19-1");

  // XC-1797, extract and decode actual target url from referrer parameter in targetUrl, only accept the url when the domain of referrer parameter belongs to ebay sites
  // XC-3349, for native uri with Chocolate parameters, re-construct Chocolate url based on native uri and track (only support /itm page)
  public Event parseCustomizedSchemeEvent(String targetUrl, String referer) {
    targetUrl = handleDuplicatedParams(targetUrl);
    UriComponents deeplinkUriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

    MultiValueMap<String, String> deeplinkParameters = deeplinkUriComponents.getQueryParams();

    if (deeplinkParameters.containsKey(MKEVT) && deeplinkParameters.containsKey(MKCID)) {
      String mkevt = deeplinkParameters.get(Constants.MKEVT).get(0);
      ChannelIdEnum channelType = ChannelIdEnum.parse(deeplinkParameters.get(Constants.MKCID).get(0));

      if (mkevt.equals(CLICKEVENTFLAG) && channelType != null) {
        MonitorUtil.info("IncomingAppDeepLinkWithChocolateParams", 1, Field.of(CHANNEL_TYPE, channelType.toString()));
      } else {
        LOGGER.warn(Errors.ERROR_INVALID_CHOCOLATE_PARAMS_DEEPLINK);
        MonitorUtil.info(Errors.ERROR_INVALID_CHOCOLATE_PARAMS_DEEPLINK);
        return null;
      }

      // this case is only for ePN facebook publisher
      if (channelType == ChannelIdEnum.EPN && deeplinkParameters.containsKey(MKRID) && deeplinkParameters.containsKey(NAV) && deeplinkParameters.containsKey(ID)) {

        String pageType = deeplinkParameters.get(NAV).get(0);
        String itemId = deeplinkParameters.get(ID).get(0);

        if (pageType.equals(VIEWITEM) && !StringUtils.isEmpty(itemId)) {
          String viewItemChocolateURL = CollectionServiceUtil.constructViewItemChocolateURLForDeepLink(deeplinkParameters);
          if (!StringUtils.isEmpty(viewItemChocolateURL)) {
            MonitorUtil.info("IncomingEPNViewItemAppDeepLinkWithChocolateParamsSuccess", 1, Field.of(CHANNEL_TYPE, channelType.toString()));
            return constructDeeplinkEvent(viewItemChocolateURL, referer);
          } else {
            LOGGER.warn(Errors.ERROR_INVALID_CHOCOLATE_PARAMS_DEEPLINK);
            MonitorUtil.info(Errors.ERROR_INVALID_CHOCOLATE_PARAMS_DEEPLINK);
            return null;
          }
        }
      } else {
        MonitorUtil.info("IncomingAppDeepLinkWithChocolateParamsSuccess", 1, Field.of(CHANNEL_TYPE, channelType.toString()));
        return constructDeeplinkEvent(targetUrl, referer);
      }
    } else if (deeplinkParameters.containsKey(REFERRER)) {
      MonitorUtil.info("IncomingAppDeepLinkWithReferrerParams", 1);
      String deeplinkTargetUrl = deeplinkParameters.get(REFERRER).get(0);

      try {
        if (deeplinkTargetUrl.startsWith(HTTPS_ENCODED) || deeplinkTargetUrl.startsWith(HTTP_ENCODED)) {
          deeplinkTargetUrl = URLDecoder.decode(deeplinkTargetUrl, StandardCharsets.UTF_8.name());
        }
      } catch (Exception ex) {
        MonitorUtil.info("DecodeDeepLinkTargetUrlError");
        LOGGER.warn("Decode deeplink target url error." + ex.getMessage());
      }

      Matcher deeplinkEbaySitesMatcher = deeplinkEbaySites.matcher(deeplinkTargetUrl.toLowerCase());
      if (deeplinkEbaySitesMatcher.find()) {
        MonitorUtil.info("IncomingAppDeepLinkWithValidTargetURLSuccess");
        deeplinkTargetUrl = CollectionServiceUtil.constructReferrerChocolateURLForDeepLink(deeplinkTargetUrl);
        return constructDeeplinkEvent(deeplinkTargetUrl, referer);
      } else {
        LOGGER.warn(Errors.ERROR_INVALID_TARGET_URL_DEEPLINK);
        MonitorUtil.info(Errors.ERROR_INVALID_TARGET_URL_DEEPLINK);
        return null;
      }
    }

    return null;
  }

  private Event constructDeeplinkEvent (String targetUrl, String referer) {
    Event event = new Event();
    event.setTargetUrl(targetUrl);
    event.setReferrer(referer);
    return event;
  }

  private String handleDuplicatedParams(String url) {
    try {
      URIBuilder uriBuilder = new URIBuilder(url);
      List<NameValuePair> queryParams = uriBuilder.getQueryParams();
      if (queryParams == null || queryParams.size() == 0) {
        return url;
      }
      UriComponents deeplinkUriComponents = UriComponentsBuilder.fromUriString(url).build();
      MultiValueMap<String, String> deeplinkParameters = deeplinkUriComponents.getQueryParams();
      List<String> mkrids = deeplinkParameters.get(MKRID);
      List<String> mkcids = deeplinkParameters.get(MKCID);
      List<String> needToReplacedRid = ListUtils.retainAll(mkrids, smartBannerRids);

      if (needToReplacedRid.size() == 1 && mkcids.contains(ChannelIdEnum.SOCIAL_MEDIA.getValue())
              && mkcids.contains(ChannelIdEnum.DAP.getValue())) {
        BasicNameValuePair duplicatedCid = new BasicNameValuePair(MKCID, ChannelIdEnum.DAP.getValue());
        queryParams.add(queryParams.indexOf(duplicatedCid),
                new BasicNameValuePair(BANNERCID, ChannelIdEnum.DAP.getValue()));
        queryParams.remove(duplicatedCid);

        String ridVal = needToReplacedRid.get(0);
        BasicNameValuePair duplicatedRid = new BasicNameValuePair(MKRID, ridVal);
        queryParams.add(queryParams.indexOf(duplicatedRid), new BasicNameValuePair(BANNERRID, ridVal));
        queryParams.remove(duplicatedRid);
      }
      return uriBuilder.setParameters(queryParams).build().toString();
    } catch (Exception ex) {
      LOGGER.error("Error while handling duplicated parameters, error : ", ex);
    }
    return url;
  }
}