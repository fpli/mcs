package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.tracking.api.IRequestScopeTracker;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiangli4
 * Utility for parsing agent info to app tags. These logic will be removed after tracking team fixed the bug in
 * raptor-io. The raptor-io api won't write app tags for now, so we implement the same logic as they do.
 * <p>
 * Tags determined by Device Detection Service - DDS, RaptorUserExperienceData & Collection Service
 * https://github.corp.ebay.com/dds/RaptorDDSHandler/blob/master/README.md
 * app (also called appid)
 * Please see app tag for detailed explanation.
 * https://wiki.vip.corp.ebay.com/display/X/app+tag
 * <p>
 * Tags determined by Device Detection Service - DDS & Collection Service
 * https://github.corp.ebay.com/dds/RaptorDDSHandler/blob/master/README.md
 * If isDesktop()
 * dsktop
 * If isTablet()
 * tablet
 * If isMobile()
 * mobile
 * If isNativeApp()
 * nativeApp
 * metadataAppNameVersion
 * getAppInfo()
 * an
 * mav
 * getDeviceInfo()
 * mos
 * osv
 * res
 * dn
 * dm
 */
public class CollectionServiceUtil {

  private static String MOBILE_PHONE_WEB_APPID = "3564";
  private static String MOBILE_TABLET_WEB_APPID = "1115";
  private static String IPHONE_APPID = "1462";
  private static String IPAD_APPID = "2878";
  private static String ANDROID_APPID = "2571";
  private static final String TRANSACTION_TIMESTAMP = "transactionTimestamp";
  private static final String TRANSACTION_TYPE = "transType";
  private static final String TRANSACTION_ID = "uniqueTransactionId";
  private static final String ITEM_ID = "itemId";
  private static final String ROI_SOURCE = "roisrc";
  private static final String MPUID = "mpuid";
  private static final String BOT_USER_AGENT = "bot";
  private static final String PROMOTED_LISTINGS_SOURCE= "PromotedListings";

  // do not dedupe the item clicks from ebay special sites
  private static Pattern ebaySpecialSites = Pattern.compile("^(http[s]?:\\/\\/)?([\\w.]+\\.)?(befr|benl+\\.)?(qa\\.)?ebay\\.(be|nl|pl|ie|ph|com\\.hk|com\\.my|com\\.sg)($|/.*)", Pattern.CASE_INSENSITIVE);

  // ebay item page
  private static Pattern ebayItemPage = Pattern.compile("^(http[s]?:\\/\\/)?([\\w-.]+\\.)?ebay\\.[\\w-.]+(\\/(?=itm\\/).*)", Pattern.CASE_INSENSITIVE);

  // ebay item no title page
  private static Pattern ebayItemNoTitlePage = Pattern.compile("^(http[s]?:\\/\\/)?([\\w-.]+\\.)?ebay\\.[\\w-.]+(\\/(?=itm\\/[0-9]+\\?).*)", Pattern.CASE_INSENSITIVE);

  // referer pattern for the clicks from Promoted Listings iframe on ebay partner sites
  private static Pattern promotedListsingsRefererWithEbaySites = Pattern.compile("^(http[s]?:\\/\\/)?([\\w.]+\\.)?(qa\\.)?ebay\\.[\\w-.]+(\\/gum\\/.*)", Pattern.CASE_INSENSITIVE);

  /**
   * get app id from user agent info
   *
   * @param uaInfo UserAgentInfo object
   * @return defined id
   */
  public static String getAppIdFromUserAgent(UserAgentInfo uaInfo) {
    String appId = "";
    if (uaInfo != null) {
      if (uaInfo.isMobile() && uaInfo.requestIsMobileWeb() && !uaInfo.requestIsTabletWeb()) {
        // mobile phone web
        appId = MOBILE_PHONE_WEB_APPID;
      } else if (uaInfo.isMobile() && !uaInfo.requestIsMobileWeb() && uaInfo.requestIsTabletWeb()) {
        // mobile tablet web
        appId = MOBILE_TABLET_WEB_APPID;
      } else if (uaInfo.requestedFromSmallDevice() && uaInfo.getDeviceInfo().osiOS() && uaInfo.requestIsNativeApp()) {
        // iphone
        appId = IPHONE_APPID;
      } else if (uaInfo.requestedFromLargeDevice() && uaInfo.getDeviceInfo().osiOS() && uaInfo.requestIsNativeApp()) {
        // ipad
        appId = IPAD_APPID;
      } else if (uaInfo.getDeviceInfo() != null && uaInfo.getDeviceInfo().osAndroid() && uaInfo.requestIsNativeApp()) {
        // android
        appId = ANDROID_APPID;
      }
    }
    return appId;
  }

  /**
   * populate device tags
   *
   * @param info    user agent info
   * @param tracker the tracking tracker
   */
  public static void populateDeviceDetectionParams(UserAgentInfo info, IRequestScopeTracker tracker) {

    if (info != null) {
      tracker.addTag("app", getAppIdFromUserAgent(info), String.class);
    }
  }

  public static boolean isLongNumeric(String strNum) {
    if (StringUtils.isEmpty(strNum)) {
      return false;
    }
    try {
      Long l = Long.parseLong(strNum);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  public static String generateQueryString(ROIEvent roiEvent, Map<String, String> payloadMap, String localTimestamp, String userId) throws UnsupportedEncodingException {
    String queryString = "tranType=" +URLEncoder.encode(roiEvent.getTransType() == null ? "" : roiEvent.getTransType(), "UTF-8")
        + "&uniqueTransactionId=" + URLEncoder.encode(roiEvent.getUniqueTransactionId() == null ? "" : roiEvent.getUniqueTransactionId(), "UTF-8")
        + "&itemId=" + URLEncoder.encode(roiEvent.getItemId() == null ? "" : roiEvent.getItemId(), "UTF-8")
        + "&transactionTimestamp=" + URLEncoder.encode(roiEvent.getTransactionTimestamp() == null ? localTimestamp : roiEvent.getTransactionTimestamp(), "UTF-8");

    // If the field in payload is in {transType, uniqueTransactionId, itemId, transactionTimestamp}, don't append them into the url
    payloadMap.remove(TRANSACTION_TIMESTAMP);
    payloadMap.remove(TRANSACTION_TYPE);
    payloadMap.remove(TRANSACTION_ID);
    payloadMap.remove(ITEM_ID);

    // If MPUID is not inside payload or it's empty, generate it and set into payload
    // The format of mpuid: user_id;item_id;[transaction_id]
    // MPUID is used in imkETL process
    if (!payloadMap.containsKey(MPUID) || StringUtils.isEmpty(payloadMap.get(MPUID))) {
      String mpuid = String.format("%s;%s;%s", userId, roiEvent.getItemId(), roiEvent.getUniqueTransactionId());
      payloadMap.put(MPUID, mpuid);
    }

    // append payload fields into URI
    for (Map.Entry<String, String> entry : payloadMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      // If the value in payload is null, don't append the fields into url
      if (value != null) {
        // If payload key is mpuid, query will not encode its value, the reason is that
        // MPUID will be used in imkETL process to parse item_id and transaction_id, imkETL process will not decode
        // our query. So if MPUID is encoded in this place, it will cause split error in imkETL
        if (key.equalsIgnoreCase(MPUID)) {
          queryString = String.format("%s&%s=%s", queryString, key, value);
        } else if (isEncodedUrl(value)){
          // If payload value is encoded, query will not encode it twice
          queryString = String.format("%s&%s=%s", queryString, key, value);
        } else {
          // Other fields in payload will be encode for avoiding invalid character cause rvr_url parse error
          queryString = String.format("%s&%s=%s", queryString, URLEncoder.encode(key, "UTF-8"),
              URLEncoder.encode(value, "UTF-8"));
        }
      }
    }
    // If roi event is not checkout api source or roi source field not found, add nroi field
    // If nroi=1, process will send the event to new roi topic, this pipeline is no impact with imk table
    if ((!payloadMap.containsKey(ROI_SOURCE))
        || (!payloadMap.get(ROI_SOURCE).equals(String.valueOf(RoiSourceEnum.CHECKOUT_SOURCE.getId())))) {
      queryString = queryString + "&nroi=1";
    }
    return queryString;
  }

  private static boolean isEncodedUrl(String url) {
    return (url.startsWith("https%3A%2F%2F") || url.startsWith("http%3A%2F%2F"));
  }

  /**
   * Determine whether the click is a duplicate click from /itm page, if so, we will filter it.
   * The duplication will happen when there is no title in itm click url on mobile phone web from non-special sites
   * No filter for bot clicks
   * No filter for user clicks from special sites
   * No filter for dweb+Tablet user clicks
   * No filter for native app user clicks
   * Filter 301 for user clicks from non-special sites and mobile phone web
   */
   public static boolean isDuplicateItmClick(String marketingStatusCode, String userAgent,
                                             String targetUrl, boolean requestIsFromBot, boolean requestIsMobile, boolean requestIsMobileWeb) {
    boolean isDulicateItemClick = false;

    if (ebayItemNoTitlePage.matcher(targetUrl).find()) {
       Matcher ebaySpecialSitesMatcher = ebaySpecialSites.matcher(targetUrl);

       if (!userAgent.toLowerCase().contains(BOT_USER_AGENT) && !requestIsFromBot &&
               !ebaySpecialSitesMatcher.find() &&
               requestIsMobile && requestIsMobileWeb) {

         if (!StringUtils.isEmpty(marketingStatusCode) && marketingStatusCode.equals(Constants.NODE_REDIRECTION_STATUS_CODE)) {
           isDulicateItemClick = true;
         }

       }
    }

    return isDulicateItemClick;
  }

  /**
   * Determine whether the click is from Promoted Listings iframe on ebay partner sites
   * 1. Channel : ePN
   * 2. In the click URL, mksrc=PromotedListings
   * 3. The parameter ‘plrfr’ exists in the URL
   * 4. The original referrer of the click is eBay domain and the format is 'https://www.ebay.../gum/%'
   */
  public static boolean isEPNPromotedListingsClick(ChannelIdEnum channelType,
                                                   MultiValueMap<String, String> parameters, String originalReferer) {
    boolean isEPNPromotedListingClick = false;

    if (channelType == ChannelIdEnum.EPN &&
            parameters.containsKey(Constants.MKSRC) && parameters.get(Constants.MKSRC).get(0) != null &&
            parameters.containsKey(Constants.PLRFR) && parameters.get(Constants.PLRFR).get(0) != null) {

      // This flag is used to distinguish if the click is from Promoted Listings iframe on ebay partner sites
      String mksrc = parameters.get(Constants.MKSRC).get(0);

      // The actual referer for the clicks from Promoted Listings iframe on ebay partner sites (eg: https://www.gumtree.com%)
      String actualPromotedListingsClickReferer = parameters.get(Constants.PLRFR).get(0);

      if (mksrc.equals(PROMOTED_LISTINGS_SOURCE) &&
              promotedListsingsRefererWithEbaySites.matcher(originalReferer.toLowerCase()).find() &&
              (!StringUtils.isEmpty(actualPromotedListingsClickReferer))) {
          isEPNPromotedListingClick = true;
      }
    }
    return isEPNPromotedListingClick;
  }

  /**
   * Get the substring between start and end. Compatible with com.ebay.hadoop.udf.soj.StrBetweenEndList
   * @param url source string
   * @param start start string
   * @param end end string
   * @return substring
   */
  public static String substring(String url, String start, String end) {
    if (StringUtils.isEmpty(url)) {
      return null;
    }
    int startPos;
    int endPos;

    if (!StringUtils.isEmpty(start)) {
      startPos = url.indexOf(start);
      if (startPos < 0) {
        return null;
      } else {
        startPos += start.length();
        if (startPos == url.length()) {
          return null;
        }
      }
    } else {
      startPos = 0;
    }

    if (StringUtils.isEmpty(end)) {
      return url.substring(startPos);
    }

    endPos = url.length();
    int len = end.length();
    for (int i = 0; i < len; ++i) {
      char c = end.charAt(i);
      int l = url.indexOf(c, startPos);
      if (l != -1 && l < endPos) {
        endPos = l;
      }
    }

    return endPos > startPos ? url.substring(startPos, endPos) : null;
  }
}
