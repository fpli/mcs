/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.constant;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author xiangli4
 */
public class Constants {
  public static final String MKCID = "mkcid";
  public static final String MKRID = "mkrid";
  public static final String SEARCH_KEYWORD = "keyword";
  public static final String GCLID = "gclid";
  public static final String MKEVT = "mkevt";
  public static final String VALID_MKEVT_CLICK = "1";
  public static final String CAMPID = "campid";
  public static final String CAMPAIGN_ID = "campaignid";
  public static final String MKSID = "mksid";
  public static final String PLATFORM_NATIVE_APP = "nativeApp";
  public static final String PLATFORM_TABLET = "tablet";
  public static final String PLATFORM_MOBILE = "mobile";
  public static final String PLATFORM_DESKTOP = "dsktop";
  public static final String PLATFORM_UNKNOWN = "UNKNOWN";
  public static final String RVRID = "rvrid";
  public static final String EPAGE_REFERER = "originalRef";
  public static final String EPAGE_URL = "originalUrl";
  public static final String ADGUID = "adguid";
  public static final String GUID = "guid";
  public static final String CGUID = "cguid";
  public static final String STR_NULL = "null";

  // Request headers
  public static final String TRACKING_HEADER = "X-EBAY-C-TRACKING";
  public static final String ENDUSERCTX_HEADER = "X-EBAY-C-ENDUSERCTX";
  public static final String AUTH_HEADER = "Authorization";

  // URL related
  public static final String MKRVRID = "mkrvrid";
  public static final String REFERRER = "referrer";
  public static final String HTTPS_ENCODED = "https%3A%2F%2";
  public static final String HTTP_ENCODED = "http%3A%2F%2";
  public static final String SOJ_MPRE_TAG = "url_mpre";

  // EPN url params
  public static final String TOOL_ID = "toolid";
  public static final String CAMP_ID = "campid";

  // Email url params
  public static final String MKPID = "mkpid";
  public static final String SOJ_TAGS = "sojTags";
  public static final String SOURCE_ID = "emsid";
  public static final String EMAIL_UNIQUE_ID = "euid";
  public static final String EXPRCD_TRTMT = "ext";
  public static final String BEST_GUESS_USER = "bu";
  public static final String CAMP_RUN_DT = "crd";
  public static final String SEGMENT_NAME = "segname";
  public static final String YM_MSSG_MSTR_ID = "ymmmid";
  public static final String YM_MSSG_ID = "ymsid";
  public static final String YM_INSTC = "yminstc";
  public static final String SMS_ID = "smsid";
  public static final String CHOCO_BUYER_ACCESS_SITE_ID = "choco_bs";
  public static final String TRACKING_ID = "tracking.id";

  public static final String REFERER_HEADER = "referer";
  public static final String REFERER_HEADER_UPCASE = "Referer";

  // Adobe url params
  public static final String REDIRECT_URL_SOJ_TAG = "adcamp_landingpage";
  public static final String REDIRECT_SRC_SOJ_SOURCE = "adcamp_locationsrc";
  public static final String ADOBE_CAMP_PUBLIC_USER_ID = "pu";

  // Redirection header name
  public static final String NODE_REDIRECTION_HEADER_NAME = "X-EBAY-TRACKING-MARKETING-STATUS-CODE";
  public static final String NODE_REDIRECTION_STATUS_CODE = "301";

  // Self-service params
  public static final String SELF_SERVICE = "self_service";
  public static final String SELF_SERVICE_ID = "self_service_id";

  // Event family and action
  public static final String EVENT_FAMILY_CRM = "mktcrm";
  public static final String EVENT_ACTION = "mktc";

  // Metrics name
  public static final String CHANNEL_ACTION = "channelAction";
  public static final String CHANNEL_TYPE = "channelType";

  // Promoted Listings url params
  // mksrc is used to mark if the click is from promoted listings iframe on ebay partner site
  public static final String MKSRC = "mksrc";
  // plrfr is the actual referer for the clicks from promoted listings iframe on ebay partner site
  public static final String PLRFR = "plrfr";

  /**
   * Performance marketing channels
   */
  public static final Set<ChannelIdEnum> PM_CHANNELS = new HashSet<>(
      Arrays.asList(ChannelIdEnum.EPN, ChannelIdEnum.PAID_SEARCH,
          ChannelIdEnum.DAP, ChannelIdEnum.SOCIAL_MEDIA, ChannelIdEnum.SEARCH_ENGINE_FREE_LISTINGS)
  );

  /**
   * Email tag - param map
   */
  public static final ImmutableMap<String, String> emailTagParamMap = new ImmutableMap.Builder<String, String>()
      .put("adcamp_landingpage", "adcamp_landingpage")
      .put("adcamp_locationsrc", "adcamp_locationsrc")
      .put("adcamppu", "pu")
      .put("bu", "bu")
      .put("cbtrack", "cbtrack")
      .put("chnl", "mkcid")
      .put("crd", "crd")
      .put("cs", "cs")
      .put("ec", "ec")
      .put("emid", "bu")
      .put("emsid", "emsid")
      .put("es", "es")
      .put("euid", "euid")
      .put("exe", "exe")
      .put("ext", "ext")
      .put("nqc", "nqc")
      .put("nqt", "nqt")
      .put("osub", "osub")
      .put("placement-type", "placement-type")
      .put("rank", "rank")
      .put("rpp_cid", "rpp_cid")
      .put("segname", "segname")
      .put("yminstc", "yminstc")
      .put("ymmmid", "ymmmid")
      .put("ymsid", "ymsid")
      .build();

}
