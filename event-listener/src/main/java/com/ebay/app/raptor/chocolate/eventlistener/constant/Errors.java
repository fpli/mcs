package com.ebay.app.raptor.chocolate.eventlistener.constant;

/**
 * @author xiangli4
 */
public class Errors {
  public static final String ERROR_NO_USER_AGENT="No User-Agent found.";
  public static final String ERROR_NO_ENDUSERCTX="No X-EBAY-C-ENDUSERCTX header.";
  public static final String ERROR_INVALID_ENDUSERCTX="Invalid X-EBAY-C-ENDUSERCTX header.";
  public static final String ERROR_NO_TRACKING ="No X-EBAY-C-TRACKING header.";
  public static final String ERROR_NO_REFERER ="No Referer in header nor in post body";
  public static final String ERROR_ILLEGAL_URL = "Illegal url format.";
  public static final String ERROR_NO_QUERY_PARAMETER = "No query parameter.";
  public static final String ERROR_NO_MKEVT = "No mkevt.";
  public static final String ERROR_INVALID_MKEVT = "Invalid mkevt value.";
  public static final String ERROR_NO_MKCID = "No mkcid";
  public static final String ERROR_INVALID_MKCID = "Invalid mkcid.";
  public static final String ERROR_INVALID_ROVER_CID = "Invalid rover cid.";
  public static final String ERROR_NO_MKRID = "No mkrid.";
  public static final String ERROR_INVALID_MKRID = "Invalid mkrid.";
  public static final String ERROR_INTERNAL_SERVICE = "Internal Service Error.";

  public static final String ERROR_CONTENT = "mktCollectionSvcErrorContent";
  public static final String ERROR_DOMAIN = "marketingTrackingDomain";
}
