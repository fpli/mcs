package com.ebay.traffic.chocolate.listener.api;

/**
 * @author yimeng on 2019/2/27
 */
public class EventConstant {

  /* hard coded 1x1 gif pixel, for impression serving */
  public static byte[] pixel = { 0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, (byte) 0x80, 0x0, 0x0, (byte)  0xff, (byte)  0xff,  (byte) 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b };

  public static final String CHANNEL_ACTION = "channelAction";
  public static final String CHANNEL_TYPE = "channelType";

  public static final String MK_EVENT="mkevt";
  public static final String MK_CHANNEL_ID="mkcid";
  public static final String MK_ROTATION_ID = "mkrid";
  public static final String MK_SESSION_ID = "mksid";
  public static final String MK_LND_PAGE = "mklndp";
  public static final String SITE_ID = "siteId";
  public static final String CAMPAIGN_ID = "campid";

  public static final String ITEM = "item";
  public static final String PRODUCT = "product";

  public static final String[] EBAY_HOSTS = {
      "ebay.com",
      "ebay.co.uk",
      "ebay.com.au",
      "ebay.de",
      "ebay.fr",
      "ebay.it",
      "ebay.es",
      "ebay.at",
      "ebay.be",
      "ebay.ca",
      "ebay.cn",
      "ebay.com.hk",
      "ebay.in",
      "ebay.ie",
      "ebay.co.jp",
      "ebay.com.my",
      "ebay.nl",
      "ebay.ph",
      "ebay.pl",
      "ebay.com.sg",
      "ebay.ch",
      "ebay.co.th",
      "ebay.vn"
  };
  public static final String DEFAULT_DESTINATION = "http://www.ebay.com";
}
