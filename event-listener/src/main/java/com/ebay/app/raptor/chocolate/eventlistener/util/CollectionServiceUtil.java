package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.tracking.api.IRequestScopeTracker;

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
}
