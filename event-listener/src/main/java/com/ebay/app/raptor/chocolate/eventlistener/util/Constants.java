package com.ebay.app.raptor.chocolate.eventlistener.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiangli4
 */
public class Constants {
  public static final String CID = "cid";
  public static final String MKEVT = "mkevt";
  public static final String VALID_MKEVT = "1";
  public static final String CAMPID = "campid";
  public static final String ERROR_NO_USER_AGENT="No User-Agent header";
  public static final String ERROR_NO_ENDUSERCTX="No X-EBAY-C-ENDUSERCTX header";
  public static final String ERROR_INVALID_ENDUSERCTX="Invalid X-EBAY-C-ENDUSERCTX header";
  public static final String ERROR_NO_TRACKING ="No X-EBAY-C-TRACKING header";
  public static final String ERROR_NO_REFERER ="No Referer in header nor in post body";
  public static final String ERROR_ILLEGAL_URL = "Illegal url";
  public static final String ERROR_NO_QUERY_PARAMETER = "No query parameter";
  public static final String ERROR_NO_MKEVT = "No mkevt";
  public static final String ERROR_INVALID_MKEVT = "Invalid mkevt";
  public static final String ERROR_NO_CID = "No cid";
  public static final String ERROR_INVALID_CID = "Invalid cid";
  public static final String ERROR_INTERNAL_SERVICE = "Internal Service Error";
  public static final String ACCEPTED = "Accepted";
  public static final String PLATFORM_MOBILE = "MOBILE";
  public static final String PLATFORM_DESKTOP = "DESKTOP";
  public static final String PLATFORM_UNKNOWN = "UNKNOWN";
  public static final Map<String, Integer>  errorMessageMap = new HashMap<>();

  static {
    errorMessageMap.put(ERROR_NO_USER_AGENT, 4000);
    errorMessageMap.put(ERROR_NO_ENDUSERCTX, 4001);
    errorMessageMap.put(ERROR_NO_TRACKING, 4002);
    errorMessageMap.put(ERROR_NO_REFERER, 4003);
    errorMessageMap.put(ERROR_ILLEGAL_URL, 4004);
    errorMessageMap.put(ERROR_NO_QUERY_PARAMETER, 4005);
    errorMessageMap.put(ERROR_NO_MKEVT, 4006);
    errorMessageMap.put(ERROR_INVALID_MKEVT, 4007);
    errorMessageMap.put(ERROR_INVALID_ENDUSERCTX, 4008);
    errorMessageMap.put(ERROR_INTERNAL_SERVICE, 5000);

  }
}
