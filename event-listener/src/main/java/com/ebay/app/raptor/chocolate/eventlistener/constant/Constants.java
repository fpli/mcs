package com.ebay.app.raptor.chocolate.eventlistener.constant;

import com.google.common.collect.ImmutableMap;

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
  public static final String MKSID = "mksid";
  public static final String PLATFORM_NATIVE_APP = "nativeApp";
  public static final String PLATFORM_TABLET = "tablet";
  public static final String PLATFORM_MOBILE = "mobile";
  public static final String PLATFORM_DESKTOP = "dsktop";
  public static final String PLATFORM_UNKNOWN = "UNKNOWN";
  public static final String RVRID = "rvrid";
  public static final String EPAGE_REFERER = "originalRef";
  public static final String ADGUID = "adguid";
  public static final String GUID = "guid";

  public static final String MKRVRID = "mkrvrid";
  public static final String REFERRER = "referrer";

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

  public static final String REFERER_HEADER = "referer";
  public static final String REFERER_HEADER_UPCASE = "Referer";

  // Adobe url params
  public static final String REDIRECT_URL_SOJ_TAG = "adcamp_landingpage";
  public static final String REDIRECT_SRC_SOJ_SOURCE = "adcamp_locationsrc";
  public static final String ADOBE_CAMP_PUBLIC_USER_ID = "pu";

  // Mobile Notification name and soj tags
  public static final String NOTIFICATION_ID = "nid";
  public static final String NOTIFICATION_TYPE = "ntype";
  public static final String NOTIFICATION_ACTION = "pnact";
  public static final String USER_NAME = "user_name";
  public static final String MC3_MSSG_ID = "mc3id";
  public static final String ITEM_ID = "itm";
  public static final String NOTIFICATION_TYPE_EVT = "evt";

  // Redirection header name
  public static final String NODE_REDIRECTION_HEADER_NAME = "x-ebay-tracking-marketing-status-code";

  // Self-service params
  public static final String SELF_SERVICE = "self_service";
  public static final String SELF_SERVICE_ID = "self_service_id";

  // Event family and action
  public static final String EVENT_FAMILY_CRM = "mktcrm";
  public static final String EVENT_ACTION = "mktc";

  // Metrics name
  public static final String CHANNEL_ACTION = "channelAction";
  public static final String CHANNEL_TYPE = "channelType";

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
      .put("sid", "emsid")
      .put("yminstc", "yminstc")
      .put("ymmmid", "ymmmid")
      .put("ymsid", "ymsid")
      .build();

}
