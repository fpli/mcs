package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.sojourner.common.sojlib.SOJUtil;
import org.apache.commons.lang.StringUtils;

/**
 * Created by jialili1 on 9/8/20
 *
 * Parse cobrand from user agent
 * 0 - Core ebay Site
 * 6 - Mobile Apps
 * 7 - Mobile traffic on Core eBay Site
 */
public class CobrandParser {
  private static Integer[] mobileAppId = {1462, 3564, 2571, 2878, 2573, 3882, 10509, 2410, 2805, 3833, 1099, 3653,
      1115, 1160, 1161, 4290};
  private static String[] startPatternList = {"HTC", "MOT-", "Terra/", "eBayWinPhoCore/"};
  private static String[] indexPatternList = {"iPhone", "iPad", "iPod", "AtomicBrowser/", "Android", "BlackBerry", "BB10",
      "Symbian", "SymbOS", "Opera Mini", "Opera Mobi", "MIDP", " HTC ", "webOS", "Zune ", " ZuneWP7", "BREW ",
      "Maemo Browser"};
  private static String[] matchPatternList = {"Mobile#Safari"};
  private static String[][] multiMatchPatternArray = new String[matchPatternList.length][];

  public CobrandParser() {
    for (int i = 0; i < matchPatternList.length; ++i) {
      multiMatchPatternArray[i] = matchPatternList[i].split("#");
    }
  }

  /**
   * Parse cobrand id
   */
  public static String parse(String appId, String agent) {
    int cobrand = 0;

    if (!StringUtils.isEmpty(appId) && isMobileAppId(Integer.valueOf(appId))) {
      cobrand = 6;
    } else if (isMobileEvent(agent)) {
      cobrand = 7;
    }

    return String.valueOf(cobrand);
  }

  /**
   * Check if app id is a mobile type
   */
  private static boolean isMobileAppId(int appId) {
    for (int id : mobileAppId) {
      if (id == appId) {
        return true;
      }
    }

    return false;
  }

  /**
   * Check if the event is from mobile, using agent info
   */
  private static boolean isMobileEvent(String agent) {
    if (StringUtils.isNotBlank(agent)) {
      for (String startPattern : startPatternList) {
        if (agent.startsWith(startPattern)) {
          return true;
        }
      }

      for (String indexPattern : indexPatternList) {
        if (agent.contains(indexPattern)) {
          return true;
        }
      }

      for (int i = 0; i < multiMatchPatternArray.length; ++i) {
        boolean mobileFlag = false;
        int index = -1;
        if (multiMatchPatternArray[i][0].startsWith("^")) {
          if (agent.startsWith(multiMatchPatternArray[i][0].substring(1))) {
            mobileFlag = true;
          }
        } else {
          index = agent.indexOf(multiMatchPatternArray[i][0]);
          if (index != -1) {
            mobileFlag = true;
          }
        }

        if (mobileFlag) {
          for(int j = 1; j < multiMatchPatternArray[i].length; ++j) {
            index = agent.indexOf(multiMatchPatternArray[i][j], index);
            if (index == -1) {
              mobileFlag = false;
              break;
            }
          }
        }

        if (mobileFlag) {
          return true;
        }
      }

      String browserActualType = SOJUtil.getBrowserActualType(agent);
      if (browserActualType != null && browserActualType.toUpperCase().startsWith("Mobile:".toUpperCase())) {
        return true;
      }

      String osType = SOJUtil.getOsType(agent);
      if (osType != null && (osType.toUpperCase().contains("Android".toUpperCase()) ||
          osType.toUpperCase().contains("RIM OS".toUpperCase()) ||
          osType.equalsIgnoreCase("Windows Phone"))) {
        return true;
      }

      String hwType = SOJUtil.getHwType(agent);
      if (hwType != null && (hwType.toUpperCase().startsWith("Mobile:") ||
          hwType.equalsIgnoreCase("iPhone") || hwType.equalsIgnoreCase("iPad") ||
          hwType.equalsIgnoreCase("iPod Touch") || hwType.equalsIgnoreCase("iPod"))) {
        return true;
      }
    }

    return false;
  }
}
