package com.ebay.traffic.chocolate.flink.nrt.constant;

public enum UserAgentEnum {
  MSIE("msie", 2),
  FIREFOX("firefox", 5),
  CHROME("chrome", 11),
  SAFARI("safari", 4),
  OPERA("opera", 7),
  NETSCAPE("netscape", 1),
  NAVIGATOR("navigator", 1),
  AOL("aol", 3),
  MAC("mac", 8),
  MSNTV("msntv", 9),
  WEBTV("webtv", 6),
  TRIDENT("trident", 2),
  BINGBOT("bingbot", 12),
  ADSBOT_GOOGLE("adsbot-google", 19),
  UCWEB("ucweb", 25),
  FACEBOOKEXTERNALHIT("facebookexternalhit", 20),
  DVLVIK("dvlvik", 26),
  AHC("ahc", 13),
  TUBIDY("tubidy", 14),
  ROKU("roku", 15),
  YMOBILE("ymobile", 16),
  PYCURL("pycurl", 17),
  DAILYME("dailyme", 18),
  EBAYANDROID("ebayandroid", 21),
  EBAYIPHONE("ebayiphone", 22),
  EBAYIPAD("ebayipad", 23),
  EBAYWINPHOCORE("ebaywinphocore", 24),
  NULL_USERAGENT("NULL_USERAGENT", 10),
  UNKNOWN_USERAGENT("UNKNOWN_USERAGENT", -99);

  private final String name;
  private final Integer id;

  UserAgentEnum(final String name, final Integer id) {
    this.name = name;
    this.id = id;
  }

  public String getName() {
    return this.name;
  }

  public Integer getId() {
    return this.id;
  }
}
