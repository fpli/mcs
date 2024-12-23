package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.RoiTransactionEnum;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.chocolate.utp.common.EmailPartnerIdEnum;
import com.ebay.traffic.monitoring.Field;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.*;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionServiceUtil.class);

  private static final String SELLER_ACCEPT_OFFER = "SELLER_ACCEPT_OFFER";
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
  private static final String PROMOTED_LISTINGS_SOURCE = "PromotedListings";
  private static final String CHECKOUT_API_USER_AGENT = "checkoutApi";
  private static final String DEEP_LINK_WITH_CHOCO_PARAMS = "chocodeeplink";
  private static final String DEEP_LINK_WITH_REFERRER_PARAMS = "referrerdeeplink";
  private static final String PRE_INSTALL_APP_RLUTYPE = "1";
  private static final String PRE_INSTALL_APP_USECASE = "prm";
  private static final String CLICK_EVENT_FLAG = "1";
  private static final String PRM_CLICK_ROTATION_ID = "14362-130847-18990-0";
  private static final String ADOBE_PARTNER_NAME = "adobe";
  private static final String THIRD_PARTY = "third_party";
  private static final List<String> THIRD_PARTY_VALUE = Arrays.asList("true");
  private static final List<String> BOT_LIST = Arrays.asList("bot", "proxy", "Mediapartners-Google",
          "facebookexternalhit", "aiohttp", "python-requests", "axios", "Go-http-client", "spider", "curl", "Tumblr");

  private static final String FLEX_FIELD_2 = "ff2";
  private static final String SALE_TYPE_ID = "saleTypeId";
  private static final String IS_COMMITTED = "is_committed";
  private static final String ORDER_TYPE = "order_type";

  private static final String BES_SRC = "1";
  private static final String TXNFLOW = "TXNFLOW";
  private static final String CHECKOUT = "CHECKOUT";
  private static final String SCO = "SELLER";
  private static final String STORE_FIXED_PRICE = "7";
  private static final String BASIC_FIXED_PRICE = "9";
  private static final String TRUE_FLAG = "1";
  private static final String FALSE_FLAG = "0";


  // do not dedupe the item clicks from ebay special sites
  private static Pattern ebaySpecialSites = Pattern.compile("^(http[s]?:\\/\\/)?([\\w.]+\\.)?(befr|benl+\\.)?(qa\\.)?ebay\\.(be|nl|pl|ie|ph|com\\.hk|com\\.my|com\\.sg)($|/.*)", Pattern.CASE_INSENSITIVE);

  // ebay item page
  private static Pattern ebayItemPage = Pattern.compile("^(http[s]?:\\/\\/)?([\\w-.]+\\.)?ebay\\.[\\w-.]+(\\/(?=itm\\/).*)", Pattern.CASE_INSENSITIVE);

  // ebay item no title page
  private static Pattern ebayItemNoTitlePage = Pattern.compile("^(http[s]?:\\/\\/)?([\\w-.]+\\.)?ebay\\.[\\w-.]+(\\/(?=itm\\/[0-9]+\\?).*)", Pattern.CASE_INSENSITIVE);

  // referer pattern for the clicks from Promoted Listings iframe on ebay partner sites
  private static Pattern promotedListsingsRefererWithEbaySites = Pattern.compile("^(http[s]?:\\/\\/)?([\\w.]+\\.)?(qa\\.)?ebay\\.[\\w-.]+(\\/gum\\/.*)", Pattern.CASE_INSENSITIVE);

  private static final Pattern VALID_GUID_PATTERN = Pattern.compile("^[0-9A-Za-z]*$");

  private static final List<String> PAGE_WHITELIST = Arrays.asList("cnt", "seller", "rxo", "ws", "bo", "cart", "pages");

  private static final List<String> ADOBE_PAGE_WHITELIST = Arrays.asList("itm");

  private static final List<String> REFERER_WHITELIST = Arrays.asList(
          "https://ebay.mtag.io", "https://ebay.pissedconsumer.com", "https://secureir.ebaystatic.com", "https://mesgmy.ebay", "https://mesg.ebay", "https://m.ebay", "https://ocsnext.ebay",
          "http://ebay.mtag.io", "http://ebay.pissedconsumer.com", "http://secureir.ebaystatic.com", "http://mesgmy.ebay", "http://mesg.ebay", "http://m.ebay", "http://ocsnext.ebay");

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
   * Check platform by user agent
   */
  public static String getPlatform(UserAgentInfo agentInfo) {
    String platform = Constants.PLATFORM_UNKNOWN;
    if (agentInfo.isDesktop()) {
      platform = Constants.PLATFORM_DESKTOP;
    } else if (agentInfo.isTablet()) {
      platform = Constants.PLATFORM_TABLET;
    } else if (agentInfo.isMobile()) {
      platform = Constants.PLATFORM_MOBILE;
    } else if (agentInfo.isNativeApp()) {
      platform = Constants.PLATFORM_NATIVE_APP;
    }

    return platform;
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

  public static boolean isIntegerNumeric(String strNum) {
    if (StringUtils.isEmpty(strNum)) {
      return false;
    }
    try {
      Integer i = Integer.parseInt(strNum);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  public static String generateQueryString(ROIEvent roiEvent, Map<String, String> payloadMap, String localTimestamp,
                                           String userId) throws UnsupportedEncodingException {
    String queryString = "tranType="
        + URLEncoder.encode(roiEvent.getTransType() == null ? "" : roiEvent.getTransType(), "UTF-8")
        + "&uniqueTransactionId="
        + URLEncoder.encode(roiEvent.getUniqueTransactionId() == null ? "" : roiEvent.getUniqueTransactionId(), "UTF-8")
        + "&itemId="
        + URLEncoder.encode(roiEvent.getItemId() == null ? "" : roiEvent.getItemId(), "UTF-8")
        + "&transactionTimestamp="
        + URLEncoder.encode(roiEvent.getTransactionTimestamp() == null ? localTimestamp : roiEvent.getTransactionTimestamp(), "UTF-8");

    // If the field in payload is in {transType, uniqueTransactionId, itemId, transactionTimestamp},
    // don't append them into the url
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
        } else if (isEncodedUrl(value)) {
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
   *
   * @param url   source string
   * @param start start string
   * @param end   end string
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

  /**
   * for native uri with Chocolate parameters, re-construct Chocolate url bases on native uri (only support /itm page)
   */
  public static String constructViewItemChocolateURLForDeepLink(MultiValueMap<String, String> deeplinkParamMap) {
    String viewItemChocolateURL = "";

    try {
      String rotationId = deeplinkParamMap.get(MKRID).get(0);
      String clientId = parseClientIdFromRotation(rotationId);
      String urlHost = clientIdHostMap.getOrDefault(clientId, "");

      if (!StringUtils.isEmpty(urlHost)) {
        URIBuilder deeplinkURIBuilder = new URIBuilder(urlHost);
        String deeplinkURIPath = ITEM_TAG + "/" + deeplinkParamMap.get(ID).get(0);
        deeplinkURIBuilder.setPath(deeplinkURIPath);

        for (Map.Entry<String, List<String>> entry : deeplinkParamMap.entrySet()) {
          String key = entry.getKey();
          if (!key.equals(NAV) && !key.equals(ID) && !key.equals(REFERRER)) {
            deeplinkURIBuilder.addParameter(key, entry.getValue().get(0));
          }
        }
        // this parameter is used to mark the click whose original url is custom uri with Chocolate parameters
        deeplinkURIBuilder.addParameter(FLEX_FLD_17_TXT, DEEP_LINK_WITH_CHOCO_PARAMS);
        viewItemChocolateURL = deeplinkURIBuilder.build().toString();
      }
    } catch (Exception e) {
      LOGGER.error("Construct view item chocolate URL for deeplink error." + e.getMessage());
      return "";
    }

    return viewItemChocolateURL;
  }

  /**
   * for native uri which has valid chocolate url in referrer parameter, append special flag to mark this kind of clicks
   */
  public static String constructReferrerChocolateURLForDeepLink(String originalChocolateURL) {
    String targetURL = originalChocolateURL;

    try {
      URIBuilder targetUriBuilder = new URIBuilder(originalChocolateURL);
      targetUriBuilder.addParameter(FLEX_FLD_17_TXT, DEEP_LINK_WITH_REFERRER_PARAMS);
      targetURL = targetUriBuilder.build().toString();
    } catch (Exception e) {
      LOGGER.error("Construct referrer chocolate URL for deeplink error." + e.getMessage());
      return originalChocolateURL;
    }

    return targetURL;
  }

  /**
   * extract client id from rotation id
   */
  public static String parseClientIdFromRotation(String rotationId) {
    String clientId = "999";

    if (!StringUtils.isEmpty(rotationId)) {
      String[] rotationParts = rotationId.split("-");
      if (rotationParts.length == 4) {
        clientId = rotationParts[0];
      }
    }

    return clientId;
  }

  /**
   * Determine whether the click is from Checkout API
   * If so, don't track into ubi
   */
  public static Boolean isClickFromCheckoutAPI(ChannelType channelType, IEndUserContext endUserContext) {
    boolean isClickFromCheckoutAPI = false;
    try {
      if (channelType == ChannelType.EPN && endUserContext.getUserAgent().equals(CHECKOUT_API_USER_AGENT)) {
        isClickFromCheckoutAPI = true;
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click from Checkout API error");
      MonitorUtil.info("DetermineCheckoutAPIClickError");
    }
    return isClickFromCheckoutAPI;
  }

  /**
   * Determine whether the roi is from Checkout API
   * If so, don't track into ubi
   */
  public static Boolean isROIFromCheckoutAPI(Map<String, String> roiPayloadMap, IEndUserContext endUserContext) {
    boolean isROIFromCheckoutAPI = false;
    if (roiPayloadMap.containsKey(ROI_SOURCE)) {
      if (roiPayloadMap.get(ROI_SOURCE).equals(String.valueOf(RoiSourceEnum.CHECKOUT_SOURCE.getId()))
          && endUserContext.getUserAgent().equals(CHECKOUT_API_USER_AGENT)) {
        isROIFromCheckoutAPI = true;
      }
    }
    return isROIFromCheckoutAPI;
  }

  /**
   * Determine if it is a valid roi for UBI
   * @param roiPayload
   * @return
   */
  public static Boolean isValidROI(Map<String, String> roiPayload) {
    if (roiPayload == null) {
      return false;
    }

    String roiSrc = roiPayload.get(ROI_SOURCE);
    String publisher = roiPayload.get(FLEX_FIELD_2); // if roisrc = 1, ff2 is not null; else if roisrc <> 1, ff2 is null
    String saleTypeId = roiPayload.get(SALE_TYPE_ID); // if roisrc = 1, saleTypeId is not null; else if roisrc <> 1, saleTypeId is null
    String isCommitted = roiPayload.get(IS_COMMITTED); // if roisrc = 1 and ff2 = CHECKOUT, is_committed is not null
    String orderType = roiPayload.get(ORDER_TYPE); // when roisrc = 1 and ff2 = CHECKOUT. order_type is not null
    // do not filter null roisrc ff2 tag events
    if (roiSrc == null || publisher == null) {
      return true;
    }
    // not from bes src
    if (!BES_SRC.equals(roiSrc)) {
      return true;
    }
    // from bes source, but not from ops side, and not seller-accept-offer
    if (BES_SRC.equals(roiSrc) && !publisher.startsWith(CHECKOUT) && !isSellerAcceptOffer(roiPayload)) {
      return true;
    }
    // from bes ops source, but no IS_COMMITTED or ORDER_TYPE tag
    if (BES_SRC.equals(roiSrc) && publisher.startsWith(CHECKOUT) && (isCommitted == null || orderType == null)) {
      return true;
    }
    // from bes ops source, and un committed
    if (BES_SRC.equals(roiSrc) && publisher.startsWith(CHECKOUT) && FALSE_FLAG.equals(isCommitted)) {
      return true;
    }
    // from bes ops side, and committed sco fixed_price
    if (BES_SRC.equals(roiSrc) && publisher.startsWith(CHECKOUT) &&
            (TRUE_FLAG.equals(isCommitted) && SCO.equals(orderType) && (STORE_FIXED_PRICE.equals(saleTypeId) || BASIC_FIXED_PRICE.equals(saleTypeId)))) {
      return true;
    }

    return false;
  }

  private static boolean isSellerAcceptOffer(Map<String, String> roiPayload) {
    return SELLER_ACCEPT_OFFER.equals(roiPayload.get(StringConstants.SALE_TYPE_FLOW));
  }

  /**
   * Check if the click is from UFES
   */
  public static Boolean isFromUFES(Map<String, String> headers) {
    return headers.containsKey(Constants.IS_FROM_UFES_HEADER)
        && "true".equals(headers.get(Constants.IS_FROM_UFES_HEADER));
  }
  /*
   * determine if the ROI is generated from pre-install App on Android
   */
  public static boolean isPreinstallROI(Map<String, String> roiPayloadMap, String transType) {
    boolean isPreInstallROI = false;

    String mppid = roiPayloadMap.getOrDefault(MPPID, "");
    String rlutype = roiPayloadMap.getOrDefault(RLUTYPE, "");
    String usecase = roiPayloadMap.getOrDefault(USECASE, "");

    RoiTransactionEnum roiTransactionEnum = RoiTransactionEnum.getByTransTypeName(transType);

    if (!StringUtils.isEmpty(mppid) && rlutype.equals(PRE_INSTALL_APP_RLUTYPE)
         && usecase.equals(PRE_INSTALL_APP_USECASE) && PRE_INSTALL_ROI_TRANS_TYPES.contains(roiTransactionEnum)) {
      isPreInstallROI = true;
    }

    return isPreInstallROI;
  }

  /**
   * Mock click URL if we receive ROI event from pre-install App on Android (XC-3464)
   */
  public static String createPrmClickUrl(Map<String, String> roiPayloadMap, IEndUserContext endUserContext) {
    String prmClickUrl = "";

    String mppid = roiPayloadMap.getOrDefault(MPPID, "");
    String siteId = roiPayloadMap.getOrDefault(SITEID, "0");
    String clickUrlHost = siteIdHostMap.getOrDefault(siteId, "https://www.ebay.com");

    try {
      if (!StringUtils.isEmpty(clickUrlHost)) {
        URIBuilder clickURIBuilder = new URIBuilder(clickUrlHost);
        clickURIBuilder.addParameter(MKEVT, CLICK_EVENT_FLAG);
        clickURIBuilder.addParameter(MKCID, ChannelIdEnum.DAP.getValue());
        clickURIBuilder.addParameter(MKRID, PRM_CLICK_ROTATION_ID);
        clickURIBuilder.addParameter(MPPID, URLEncoder.encode(mppid, "UTF-8"));
        clickURIBuilder.addParameter(RLUTYPE, PRE_INSTALL_APP_RLUTYPE);
        clickURIBuilder.addParameter(SITE, URLEncoder.encode(siteId, "UTF-8"));

        if (endUserContext.getDeviceId() != null) {
          clickURIBuilder.addParameter(UDID, URLEncoder.encode(endUserContext.getDeviceId(), "UTF-8"));
        }

        prmClickUrl = clickURIBuilder.build().toString();
      }
    } catch (Exception ex) {
      LOGGER.error("Construct dummy click for pre-install App ROI error." + ex.getMessage());
      return "";
    }

    return prmClickUrl;
  }

  /**
   * Is facebook prefetch enabled
   * @param requestHeaders http request headers
   * @return enabled or not
   */
  public static boolean isFacebookPrefetchEnabled(Map<String, String> requestHeaders) {
    String facebookprefetch = requestHeaders.get("X-Purpose");
    return facebookprefetch != null && "preview".equals(facebookprefetch.trim());
  }

  /**
   * The referer belongs to ebay site if user clicks some ulk links and redirects to the pages which UFES supports on iOS
   * The referer is always ebay site for the static page (pages.ebay)
   * So add a page whitelist to avoid rejecting these clicks
   * @param finalUrl finalUrl
   * @return in whitelist or not
   */
  public static boolean inPageWhitelist(String finalUrl) {
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(finalUrl).build();
    List<String> pathSegments = uriComponents.getPathSegments();

    if (pathSegments.isEmpty() || PAGE_WHITELIST.contains(pathSegments.get(0))) {
      return true;
    }

    return false;
  }

  /**
   * The ebaysites pattern will treat ebay.abcd.com and ebaystatic as ebay site.
   * In Customer Marketing channels, there are some domains whose clicks' referer always belongs to ebay site.
   * So add a whitelist to handle these cases.
   * @param referer referer
   * @return in whitelist or not
   */
  public static boolean inRefererWhitelist(String referer) {
    String lowerCase = referer.toLowerCase();
    for (String referWhitelist : REFERER_WHITELIST) {
      if (lowerCase.startsWith(referWhitelist)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determine whether the click is a duplicate click caused by ULK link
   * 1. click url is ebay://
   * 2. referer is ulk link
   * 3. user agent is iOS
   * If so, send to internal topic
   */
  public static boolean isUlkDuplicateClick(ChannelType channelType, String referer, String finalUrl, UserAgentInfo userAgentInfo) {
    boolean isULKDuplicateClick = false;

    try {
      Matcher deeplinkSitesMatcher = deeplinksites.matcher(finalUrl.toLowerCase());
      Matcher ulkSitesMatcher = ulksites.matcher(referer.toLowerCase());

      if (deeplinkSitesMatcher.find() && ulkSitesMatcher.find()
              && userAgentInfo.getDeviceInfo().osiOS() && userAgentInfo.requestIsNativeApp()
              && (userAgentInfo.requestedFromSmallDevice() || userAgentInfo.requestedFromLargeDevice())) {
        isULKDuplicateClick = true;
        MonitorUtil.info("FilteredULKDuplicateClick", 1, Field.of(CHANNEL_TYPE, channelType.toString()));
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click belongs to ulk duplicate click error");
      MonitorUtil.info("DetermineULKDuplicateClickError");
    }

    return isULKDuplicateClick;
  }

  /**
   * Identify third party clicks
   * @param url
   * @return
   */
  public static boolean isThirdPartyClick(String url) {
    if (StringUtils.isEmpty(url)) {
      return false;
    }
    if (url.contains(".ebay.")) {
      return false;
    }
    if (url.trim().startsWith("ebay://")) {
      return false;
    }
    if (url.trim().startsWith("padebay://")) {
      return false;
    }

    return true;
  }

  /**
   * Identify third party clicks
   * @param parameters
   * @return
   */
  public static boolean isThirdParityClick(MultiValueMap<String, String> parameters){
    List<String> third_party_values = parameters.get(THIRD_PARTY);
    if (CollectionUtils.isNotEmpty(third_party_values) && "true".equals(third_party_values.get(0))) {
      return true;
    }
    return false;
  }

  /**
   * The referer belongs to ebay site if user clicks some ulk links and redirects to the pages which UFES supports on iOS
   * To make sure there is no impact on the original filtered internal clicks, add some page whitelist filter logic only for Adobe
   * @param finalUrl
   * @param referer
   * @param userAgentInfo
   * @return in AdobePageWhitelist or not
   */
  public static boolean inAdobePageWhitelist(ChannelType channelType, String referer, String finalUrl, UserAgentInfo userAgentInfo) {
    UriComponents finalUrlComponents = UriComponentsBuilder.fromUriString(finalUrl).build();
    MultiValueMap<String, String> finalUrlParameters = finalUrlComponents.getQueryParams();

    if (!finalUrlParameters.isEmpty()) {
      String partner = EmailPartnerIdEnum.parse(finalUrlParameters.getFirst(Constants.MKPID));

      if (ChannelType.MRKT_EMAIL == channelType && ADOBE_PARTNER_NAME.equals(partner)
              && userAgentInfo.isMobile() && (userAgentInfo.requestIsMobileWeb() || userAgentInfo.requestIsTabletWeb())) {
        UriComponents refererComponents = UriComponentsBuilder.fromUriString(referer).build();
        List<String> refererPathSegments = refererComponents.getPathSegments();

        if (refererPathSegments.isEmpty()) {
          List<String> finalUrlPathSegments = finalUrlComponents.getPathSegments();
          if (!finalUrlPathSegments.isEmpty() && ADOBE_PAGE_WHITELIST.contains(finalUrlPathSegments.get(0))) {
            MonitorUtil.info("ClickInAdobePageWhitelist", 1);
            return true;
          }
        }
      }
    }

    return false;
  }

  /**
   * Replace one url parameter
   * @return
   */
  public static String replaceUrlParam(String url, String param, String value) {
    UriComponentsBuilder urlBuilder = UriComponentsBuilder.fromUriString(url);
    urlBuilder.replaceQueryParam(param, value);
    return urlBuilder.build().toUriString();
  }

  /**
   * Bot detection by user agent
   *
   * @param userAgent user agent
   */
  public static boolean isBot(String userAgent) {
    if (org.apache.commons.lang.StringUtils.isNotEmpty(userAgent)) {
      String userAgentLower = userAgent.toLowerCase();
      for (String botKeyword : BOT_LIST) {
        if (userAgentLower.contains(botKeyword.toLowerCase())) {
          return true;
        }
      }
    }

    return false;
  }
  
  /**
   * handle "Mobile Deep Link for Bank of America"
   * @param channelType channelType
   * @param parameters parameters: decode from url
   * @return in whitelist or not
   */
  public static boolean inSpecialCase(ChannelType channelType, MultiValueMap<String, String> parameters) {
    if(parameters == null || parameters.isEmpty()) {
      return false;
    }
    String campaignId = parameters.getFirst(CAMP_ID);
    String ulNoapp = parameters.getFirst(UL_NOAPP);
    if(channelType == ChannelType.EPN && "5339006474".equals(campaignId) && "true".equalsIgnoreCase(ulNoapp)) {
      return true;
    }
    return false;
  }

  public static boolean filterInvalidGuidByRequestHeaders(Map<String, String> requestHeaders, String actionType, String channelType) {
    if (MapUtils.isEmpty(requestHeaders)) {
      MonitorUtil.info("InvalidGuid", 1, Field.of(CHANNEL_ACTION, actionType),
              Field.of(CHANNEL_TYPE, channelType));
      return true;
    }
    String trackingHeader = requestHeaders.get(TRACKING_HEADER);
    String guid = HttpRequestUtil.getHeaderValue(trackingHeader, GUID);
    if (org.apache.commons.lang3.StringUtils.isBlank(guid) || guid.length() != 32 || !VALID_GUID_PATTERN.matcher(guid).matches()) {
      MonitorUtil.info("InvalidGuid", 1, Field.of(CHANNEL_ACTION, actionType),
              Field.of(CHANNEL_TYPE, channelType));
      return true;
    }
    return false;
  }

}
