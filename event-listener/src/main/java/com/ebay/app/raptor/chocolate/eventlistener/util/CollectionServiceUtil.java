package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.tracking.api.IRequestScopeTracker;
import org.springframework.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

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

  private static String MOBILE_WEB_APPID = "3564";
  private static String IPHONE_APPID = "1462";
  private static String IPAD_APPID = "2878";
  private static String ANDROID_APPID = "2571";
  private static final String TRANSACTION_TIMESTAMP = "transactionTimestamp";
  private static final String ROI_SOURCE = "roisrc";
  private static final String MPUID = "mpuid";

  /**
   * get app id from user agent info
   *
   * @param uaInfo UserAgentInfo object
   * @return defined id
   */
  public static String getAppIdFromUserAgent(UserAgentInfo uaInfo) {
    String appId = "";
    if (uaInfo != null) {
      if (uaInfo.requestedFromSmallDevice() && uaInfo.requestIsWeb()) {
        //mweb
        appId = MOBILE_WEB_APPID;
      } else if (uaInfo.requestedFromSmallDevice() && uaInfo.getDeviceInfo().osiOS() && uaInfo.requestIsNativeApp()) {
        //iphone
        appId = IPHONE_APPID;
      } else if (uaInfo.requestedFromLargeDevice() && uaInfo.getDeviceInfo().osiOS() && uaInfo.requestIsNativeApp()) {
        //ipad
        appId = IPAD_APPID;
      } else if (uaInfo.getDeviceInfo().osAndroid() && uaInfo.requestIsNativeApp()) {
        //android
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
    String queryString = "transType=" + URLEncoder.encode(roiEvent.getTransType() == null ? "" : roiEvent.getTransType(), "UTF-8")
        + "&uniqueTransactionId=" + URLEncoder.encode(roiEvent.getUniqueTransactionId() == null ? "" : roiEvent.getUniqueTransactionId(), "UTF-8")
        + "&itemId=" + URLEncoder.encode(roiEvent.getItemId() == null ? "" : roiEvent.getItemId(), "UTF-8")
        + "&transactionTimestamp=" + URLEncoder.encode(roiEvent.getTransactionTimestamp() == null ? localTimestamp : roiEvent.getTransactionTimestamp(), "UTF-8");

    // If the field in payload is in {transType, uniqueTransactionId, itemId, transactionTimestamp}, don't append them into the url
    payloadMap.remove(TRANSACTION_TIMESTAMP);
    payloadMap.remove("transType");
    payloadMap.remove("uniqueTransactionId");
    payloadMap.remove("itemId");

    // If MPUID is not inside payload, generate it and set into payload
    // The format of mpuid: user_id;item_id;[transaction_id]
    // MPUID is used in imkETL process
    if (!payloadMap.containsKey(MPUID)) {
      String mpuid = String.format("%s;%s;%s", userId, roiEvent.getItemId(), roiEvent.getUniqueTransactionId());
      payloadMap.put(MPUID, mpuid);
    }

    // append payload fields into URI
    for (String key : payloadMap.keySet()) {
      // If the value in payload is null, don't append the fields into url
      if (payloadMap.get(key) != null) {
        // If payload key is mpuid, query will not encode its value, the reason is that
        // MPUID will be used in imkETL process to parse item_id and transaction_id, imkETL process will not decode
        // our query. So if MPUID is encoded in this place, it will cause split error in imkETL
        if (key.equalsIgnoreCase(MPUID)) {
          queryString = String.format("%s&%s=%s", queryString, key, payloadMap.get(key));
        } else {
          // Other fields in payload will be encode for avoiding invalid character cause rvr_url parse error
          queryString = String.format("%s&%s=%s", queryString, URLEncoder.encode(key, "UTF-8"),
              URLEncoder.encode(payloadMap.get(key), "UTF-8"));
        }
      }
    }
    // If roi event is not checkout api source or roi source field not found, add nroi field
    // If nroi=1, process will send the event to new roi topic, this pipeline is no impact with imk table
    if (!payloadMap.containsKey(ROI_SOURCE)
        || !payloadMap.get(ROI_SOURCE).equals(String.valueOf(RoiSourceEnum.CHECKOUT_SOURCE.getId()))) {
      queryString = queryString + "&nroi=1";
    }
    return queryString;
  }

}
