package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.tracking.api.IRequestScopeTracker;

/**
 * @author xiangli4
 * Utility for parsing agent info to app tags
 */
public class CollectionServiceUtil {

  private static String MOBILE_WEB_APPID = "3564";
  private static String IPHONE_APPID = "1462";
  private static String IPAD_APPID = "2878";
  private static String ANDROID_APPID = "2571";

  /**
   * get app id from user agent info
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
    } else {
    }
    return appId;
  }

  /**
   * populate device tags
   * @param info user agent info
   * @param tracker the tracking tracker
   */
  public static void populateDeviceDetectionParams(UserAgentInfo info, IRequestScopeTracker tracker) {

    if (info != null) {
      tracker.addTag("app", getAppIdFromUserAgent(info), String.class);
      if (info.isDesktop()) {
        tracker.addTag("dsktop", true, Boolean.class);
        return;
      } else if (info.isTablet()) {
        tracker.addTag("tablet", true, Boolean.class);
      } else if (info.isMobile()) {
        tracker.addTag("mobile", true, Boolean.class);
      } else if (info.isNativeApp()) {
        tracker.addTag("nativeApp", true, Boolean.class);
        //comment metadataAppNameVersion waiting for tracking team's confirmation
        //String appNameVersion = info.getAppInfo().getAppName() + "/" + info.getAppInfo().getAppVersion();
        //tracker.addTag("metadataAppNameVersion", appNameVersion, String.class);
      }
      if (info.getAppInfo() != null) {
        tracker.addTag("an", info.getAppInfo().getAppName(), String.class);
        tracker.addTag("mav", info.getAppInfo().getAppVersion(), String.class);
      }
      if (info.getDeviceInfo() != null) {
        tracker.addTag("mos", info.getDeviceInfo().getDeviceOS(), String.class);
        tracker.addTag("osv", info.getDeviceInfo().getDeviceOSVersion(), String.class);
        tracker.addTag("res", info.getDeviceInfo().getDisplayHeight() + "x" + info.getDeviceInfo().getDisplayWidth(),
          String.class);
        tracker.addTag("dn", info.getDeviceInfo().getModel(), String.class);
        tracker.addTag("dm", info.getDeviceInfo().getManufacturer(), String.class);
      }
    }
  }
}
