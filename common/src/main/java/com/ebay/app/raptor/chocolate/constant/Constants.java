/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.constant;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMap;

import java.util.*;

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
  public static final String GUID_LIST = "guidList";
  public static final String CGUID = "cguid";
  public static final String STR_NULL = "null";
  public static final String TAG_IS_UFES = "isUfes";
  public static final String TAG_STATUS_CODE = "statusCode";
  public static final String TAG_IS_UFES_REDIRECT = "isUFESRedirect";
  public static final String TAG_IS_THIRD_PARTY = "isThirdParty";
  public static final String TAG_CHANNEL = "chnl";
  public static final String TAG_REF = "ref";
  public static final String TAG_UTP_ID = "utpid";
  public static final String TAG_EMID = "emid";
  public static final String EMAIL_USER_ID = "emailUserId";
  public static final String TAG_TRACK_ID = "trkId";
  public static final String BANNERCID = "bannercid";
  public static final String BANNERRID = "bannerrid";

  // Session relatives
  public static final String SESSION_ID = "sessionId";
  public static final String SESSION_SKEY = "sessionSkey";
  public static final String SEQ_NUM = "seqNum";
  public static final int MILLI2MICRO = 1000;
  // The offset align with UTC-7
  // 25567L *24 * 3600 * 1000 * 1000 - 7 * 3600 * 1000 * 1000;
  public static final long OFFSET = 2208963600000000L;
  public static final int SESSION_KEY_DIVISION = 100;

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
  public static final String ITEM_TAG = "itm";

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
  public static final String SEGMENT_NAME_S = "seg";
  public static final String YM_MSSG_MSTR_ID = "ymmmid";
  public static final String YM_MSSG_ID = "ymsid";
  public static final String YM_INSTC = "yminstc";
  public static final String SMS_ID = "smsid";
  public static final String CHOCO_BUYER_ACCESS_SITE_ID = "choco_bs";

  public static final String REFERER_HEADER = "referer";
  public static final String REFERER_HEADER_UPCASE = "Referer";
  public static final String X_FORWARDED_FOR = "X-Forwarded-For";

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

  // UFES header
  public static final String IS_FROM_UFES_HEADER = "x-ufes-mcs-int";
  public static final String UFES_EDGTRKSVC_HDR = "X-UFES-EDGTRKSVC-INT";

  // Deep Link native uri params
  public static final String NAV = "nav";
  public static final String ID = "id";

  public static final String IID = "iid";
  // FLEX_FLD_17_TXT is used to mark the click whose original url is custom uri (ebay://)
  public static final String FLEX_FLD_17_TXT = "ff17";

  // Pre-install App ROI params
  public static final String MPPID = "mppid";
  public static final String RLUTYPE = "rlutype";
  public static final String USECASE = "usecase";
  public static final String SITEID = "siteId";

  // Pre-install App dummy click params
  public static final String UDID = "udid";
  public static final String SITE = "site";

  // UFES redirect flag for the Rover clicks onboarded to UFES
  public static final String UFES_REDIRECT = "ufes_redirect";

  // ep's treatment id: qualified and treated
//  public static final String CXT = "cxt";
  public static final String XT = "xt";

  public static final String SLASH = "/";
  public static final String HYPHEN = "-";

  public static final String TYPE_INFO = "Info";
  public static final String STATUS_OK = "0";

  public static final String IS_BOT_TRAFFIC = "IsBotTraffic";

  /**
   * Performance marketing channels
   */
  public static final Set<ChannelIdEnum> PM_CHANNELS = new HashSet<>(
      Arrays.asList(ChannelIdEnum.EPN, ChannelIdEnum.PAID_SEARCH,
          ChannelIdEnum.DAP, ChannelIdEnum.SOCIAL_MEDIA, ChannelIdEnum.SEARCH_ENGINE_FREE_LISTINGS)
  );

  /**
   * Pre-install ROI transaction types
   */
  public static final Set<RoiTransactionEnum> PRE_INSTALL_ROI_TRANS_TYPES = new HashSet<>(
          Arrays.asList(RoiTransactionEnum.BID_MOBILEAPP, RoiTransactionEnum.BIN_MOBILEAPP,
                  RoiTransactionEnum.BO_MOBILE_APP, RoiTransactionEnum.SELL_MOBILE_APP,
                  RoiTransactionEnum.REG_MOBILE_APP, RoiTransactionEnum.REG_SELL_MOBILE_APP)
  );

  /**
   * mkt email soj tags
   */
  public static final ImmutableMultimap<String, String> mktEmailParamTags = new ImmutableMultimap.Builder<String, String>()
          .put("adcampid", ID)
          .put("adcamppu", ADOBE_CAMP_PUBLIC_USER_ID)
          .put("ch", "ch")
          .put("crd", CAMP_RUN_DT)
          .put("chnl", MKCID)
          .put("du", "du")
          .put("emid", BEST_GUESS_USER)
          .put("emsid", SOURCE_ID)
          .put("mesgId", "mesgId")
          .put("mm", "mm")
          .put("osub", "osub")
          .put("plmtId", "plmtId")
          .put("recoId", "recoId")
          .put("recoPos", "recoPos")
          .put("segname", SEGMENT_NAME)
          .put("trkId", "trkId")
          .put("url", "url")
          .put("ut", "ut")
          .put("ymuid", "ymuid")
          .put("zz", "zz")
          .put("rpp_cid", "rpp_cid")
          .put("ymcb", "ymcb")
          .put("ymdivid", "ymdivid")
          .put("ymhid", "ymhid")
          .put("ymmmid", YM_MSSG_MSTR_ID)
          .put("ymsid", YM_MSSG_ID)
          .put("yminstc", YM_INSTC)
          .put("adcamp_locationsrc", REDIRECT_SRC_SOJ_SOURCE)
          .put("cbtrack", "cbtrack")
          .put("placement-type", "placement-type")
          .put("placement-type", "pt")
          .put("rank", "rank")
          .put("sid", SOURCE_ID)
          .put("xt", XT)
          .build();

  /**
   * site email soj tags
   */
  public static final ImmutableMultimap<String, String> siteEmailParamTags = new ImmutableMultimap.Builder<String, String>()
          .put("adg", "adg")
          .put("chnl", MKCID)
          .put("ch", "ch")
          .put("crd", "crd")
          .put("du", "du")
          .put("ec", "ec")
          .put("emid", BEST_GUESS_USER)
          .put("emsid", SOURCE_ID)
          .put("eprlogid", "eprlogid")
          .put("es", "es")
          .put("exe", "exe")
          .put("ext", EXPRCD_TRTMT)
          .put("euid", EMAIL_UNIQUE_ID)
          .put("mdbreftime", "mdbreftime")
          .put("nqc", "nqc")
          .put("nqt", "nqt")
          .put("os", "os")
          .put("osub", "osub")
          .put("ot", "ot")
          .put("segname", "segname")
          .put("url", "loc")
          .put("ut", "ut")
          .put("cbtrack", "cbtrack")
          .put("placement-type", "placement-type")
          .put("placement-type", "pt")
          .put("rank", "rank")
          .put("sid", SOURCE_ID)
          .put("xt", XT)
          .build();

  /**
   * GCX email tags
   */
  public static final ImmutableMultimap<String, String> gcxEmailParamTags = new ImmutableMultimap.Builder<String, String>()
      .put("emid", BEST_GUESS_USER)
      .put("chnl", MKCID)
      .put("trkId", "trkId")
      .build();

  public static final Map<ChannelType, ImmutableMultimap<String, String>> channelParamTagMap
          = new HashMap<ChannelType, ImmutableMultimap<String, String>>() {{
    put(ChannelType.MRKT_EMAIL, mktEmailParamTags);
    put(ChannelType.MRKT_MESSAGE_CENTER, mktEmailParamTags);
    put(ChannelType.SITE_EMAIL, siteEmailParamTags);
    put(ChannelType.SITE_MESSAGE_CENTER, siteEmailParamTags);
    put(ChannelType.GCX_EMAIL, gcxEmailParamTags);
    put(ChannelType.GCX_MESSAGE_CENTER, gcxEmailParamTags);
  }};

  public static final ImmutableMultimap<String, String> socialMediaParamTags = new ImmutableMultimap.Builder<String, String>()
          .put("media", "media")
          .put("sssrc", "sssrc")
          .put("ssuid", "ssuid")
          .put("ssspo", "ssspo")
          .put("_sacat", "_sacat")
          .put("store_cat", "store_cat")
          .build();

  /**
   * client id - host map
   * this is used to construct Chocolate final landing page url when we receive the click whose original url is custom uri with Chocolate parameters
   * only support /itm page
   */
  public static final ImmutableMap<String, String> clientIdHostMap = new ImmutableMap.Builder<String, String>()
          .put("5282", "https://www.ebay.ie")
          .put("705", "https://www.ebay.com.au")
          .put("709", "https://www.ebay.fr")
          .put("1346", "https://www.ebay.nl")
          .put("3422", "https://www.ebay.com.hk")
          .put("1553", "https://www.ebay.be")
          .put("710", "https://www.ebay.co.uk")
          .put("5221", "https://www.ebay.at")
          .put("5222", "https://www.ebay.ch")
          .put("8971", "https://www.ebay.com")
          .put("724", "https://www.ebay.it")
          .put("707", "https://www.ebay.de")
          .put("3423", "https://www.ebay.com.sg")
          .put("1185", "https://www.ebay.es")
          .put("711", "https://www.ebay.com")
          .put("706", "https://www.ebay.ca")
          .put("4686", "https://www.ebay.com")
          .build();

  /**
   * site id - host map
   * this is used to construct dummy click landing page url when we receive the ROI events generated from pre-install Android App (XC-3464)
   */
  public static final ImmutableMap<String, String> siteIdHostMap = new ImmutableMap.Builder<String, String>()
          .put("0", "https://www.ebay.com")
          .put("2", "https://www.ebay.ca")
          .put("3", "https://www.ebay.co.uk")
          .put("15", "https://www.ebay.com.au")
          .put("16", "https://www.ebay.at")
          .put("23", "https://www.befr.ebay.be")
          .put("71", "https://www.ebay.fr")
          .put("77", "https://www.ebay.de")
          .put("100", "https://www.ebay.com")
          .put("101", "https://www.ebay.it")
          .put("123", "https://www.benl.ebay.be")
          .put("146", "https://www.ebay.nl")
          .put("186", "https://www.ebay.es")
          .put("193", "https://www.ebay.ch")
          .put("205", "https://www.ebay.ie")
          .put("207", "https://www.ebay.com.my")
          .put("210", "https://www.ebay.ca")
          .put("212", "https://www.ebay.pl")
          .build();
}
