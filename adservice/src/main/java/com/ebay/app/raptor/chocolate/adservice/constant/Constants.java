package com.ebay.app.raptor.chocolate.adservice.constant;

/**
 * @author Zhiyuan Wang
 * @since 2019/10/26
 */
public class Constants {
  private Constants() {
  }

  public static final String MKCID = "mkcid";
  public static final String MKRID = "mkrid";
  public static final String CGUID = "cguid";
  public static final String GUID = "guid";
  public static final String RVR_ID = "rvr_id";
  public static final String MKEVT = "mkevt";
  public static final String RVRID = "rvrid";
  public static final String SITE_ID = "siteId";
  public static final String IS_MOB_TRUE = "Y";
  public static final int UDID_MIN_LENGTH = 32;
  public static final String IS_MOB = "ismob";
  public static final String H_LAST_LOGGED_IN_USER_ID = "hLastLoggedInUserId";

  // DAP constants
  public static final String UNIQUE_DEVICE_ID = "udid";
  public static final String MKRVRID = "mkrvrid";
  public static final String UA_PARAM = "uaPrime";
  public static final String REF_DOMAIN = "refDomain";
  public static final String IPN = "ipn";
  public static final String MPT = "mpt";
  public static final String ICEP_PREFIX = "ICEP_";
  public static final int ISO_COUNTRY_CODE_LENGTH = 2;
  public static final String REF_URL = "refURL";
  public static final String ROVER_USERID = "rover_userid";

  // header name
  public static final String USER_AGENT = "User-Agent";
  public static final String REFERER = "Referer";
  public static final String HTTP_ACCEPT_LANGUAGE = "Accept-Language";

  // cookie length
  public static final int CGUID_LENGTH = 32;
  public static final int GUID_LENGTH = 32;

  // default guid
  public static final String EMPTY_GUID = "00000000000000000000000000000000";

  // metrics field
  public static final String CHANNEL_TYPE = "channelType";
  public static final String PARTNER = "partner";

  // Redirection constants
  public static final String MKPID = "mkpid";
  public static final String ADOBE_PARAMS = "adobeParams";
  public static final String LOCTATION = "loc";
  public static final String REDIRECT = "redirect";
  public static final String DEFAULT_REDIRECT_URL = "http://www.ebay.com";
  public static final String SOJ_TAGS = "sojTags";
  public static final String[] TARGET_URL_PARMS = {"mpre", "loc", "url", "URL"};

}

