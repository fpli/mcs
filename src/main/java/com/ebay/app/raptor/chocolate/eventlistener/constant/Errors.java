package com.ebay.app.raptor.chocolate.eventlistener.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiangli4
 */
public class Errors {
  public static final String ERROR_NO_USER_AGENT="NoUserAgent";
  public static final String ERROR_NO_ENDUSERCTX="NoEndUserCtx";
  public static final String ERROR_NO_TRACKING ="NoTracking";
  public static final String ERROR_NO_REFERER ="NoReferer";
  public static final String ERROR_ILLEGAL_URL = "IllegalUrl";
  public static final String ERROR_NO_QUERY_PARAMETER = "NoQueryParameter";
  public static final String ERROR_NO_MKEVT = "NoMkevt";
  public static final String ERROR_INVALID_MKEVT = "InvalidMkevt";
  public static final String ERROR_INVALID_ENDUSERCTX="InvalidEndUserCtx";
  public static final String ERROR_INTERNAL_SERVICE = "InternalServiceError";
  public static final String ERROR_NO_TARGET_URL_DEEPLINK = "NoTargetUrlDeeplink";
  public static final String ERROR_INVALID_TARGET_URL_DEEPLINK = "InvalidTargetUrlDeeplink";
  public static final String ERROR_INVALID_CHOCOLATE_PARAMS_DEEPLINK = "InvalidChocolateParamsDeeplink";
  public static final String ERROR_NO_VALID_TRACKING_PARAMS_DEEPLINK = "NoValidTrackingParamsDeeplink";
  public static final String ERROR_NO_PAGE_ID = "NoPageId";

  public static final String ERROR_NO_MKCID = "No mkcid";
  public static final String ERROR_INVALID_MKCID = "Invalid mkcid.";
  public static final String ERROR_NO_MKRID = "No mkrid.";
  public static final String ERROR_INVALID_MKRID = "Invalid mkrid.";
  public static final String ERROR_NO_MKSID = "No mksid.";
  public static final String ERROR_NO_MKPID = "No mkpid";
  public static final String ERROR_INVALID_MKPID = "Invalid mkpid.";
  public static final String ERROR_INVALID_MKSID = "Invalid mksid.";
  public static final String ERROR_CONTENT = "mktCollectionSvcErrorContent";
  public static final String ERROR_DOMAIN = "marketingTrackingDomain";

  private static final Map<String, ErrorType> ERROR_MAP = new HashMap<>();
  static {
    ERROR_MAP.put(ERROR_NO_USER_AGENT, new ErrorType(4000, ERROR_NO_USER_AGENT, "No User-Agent found."));
    ERROR_MAP.put(ERROR_NO_ENDUSERCTX, new ErrorType(4001, ERROR_NO_ENDUSERCTX, "No X-EBAY-C-ENDUSERCTX header."));
    ERROR_MAP.put(ERROR_NO_TRACKING, new ErrorType(4002, ERROR_NO_TRACKING, "No X-EBAY-C-TRACKING header."));
    ERROR_MAP.put(ERROR_NO_REFERER, new ErrorType(4003, ERROR_NO_REFERER, "No Referer in header nor in post body."));
    ERROR_MAP.put(ERROR_ILLEGAL_URL, new ErrorType(4004, ERROR_ILLEGAL_URL, "Illegal url format."));
    ERROR_MAP.put(ERROR_NO_QUERY_PARAMETER, new ErrorType(4005, ERROR_NO_QUERY_PARAMETER, "No query parameter."));
    ERROR_MAP.put(ERROR_NO_MKEVT, new ErrorType(4006, ERROR_NO_MKEVT, "No mkevt."));
    ERROR_MAP.put(ERROR_INVALID_MKEVT, new ErrorType(4007, ERROR_INVALID_MKEVT, "Invalid mkevt value."));
    ERROR_MAP.put(ERROR_INVALID_ENDUSERCTX, new ErrorType(4008, ERROR_INVALID_ENDUSERCTX, "Invalid X-EBAY-C-ENDUSERCTX header."));
    ERROR_MAP.put(ERROR_INTERNAL_SERVICE, new ErrorType(5000, ERROR_INTERNAL_SERVICE, "Internal Service Error."));
    ERROR_MAP.put(ERROR_NO_TARGET_URL_DEEPLINK, new ErrorType(4009, ERROR_NO_TARGET_URL_DEEPLINK, "No TargetUrl in Deeplink Url"));
    ERROR_MAP.put(ERROR_INVALID_TARGET_URL_DEEPLINK, new ErrorType(4010, ERROR_INVALID_TARGET_URL_DEEPLINK, "Invalid TargetUrl in Deeplink Url"));
    ERROR_MAP.put(ERROR_NO_PAGE_ID, new ErrorType(4011, ERROR_NO_PAGE_ID, "No page id"));
    ERROR_MAP.put(ERROR_NO_VALID_TRACKING_PARAMS_DEEPLINK, new ErrorType(4012, ERROR_NO_VALID_TRACKING_PARAMS_DEEPLINK, "No Valid Tracking Params in Deeplink Url"));
  }

  public static Map<String, ErrorType> getErrorMap() {
    return ERROR_MAP;
  }
}
