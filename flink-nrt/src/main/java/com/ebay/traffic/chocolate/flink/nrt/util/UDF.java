package com.ebay.traffic.chocolate.flink.nrt.util;

import org.apache.commons.lang.StringUtils;

public class UDF {
  public static String parseChannelId(String urlQueryString) {
    if (StringUtils.isEmpty(urlQueryString)) {
      return null;
    }
    String evaluate = getValueByIndex(urlQueryString, "\\?", 1);
    return getLastElement(evaluate, "/");
  }

  private static String getValueByIndex(String strVec, String vecDelimit, int vecIdx) {
    if (StringUtils.isEmpty(strVec)) {
      return null;
    }

    if (StringUtils.isEmpty(vecDelimit)) {
      return null;
    }

    vecIdx = vecIdx - 1;
    if (vecIdx < 0) {
      return null;
    }

    String[] vect;

    vect = strVec.split(vecDelimit, -1);
    if (vecIdx < vect.length)
      return vect[vecIdx];
    else {
      return null;
    }
  }

  private static String getLastElement(String strVec, String vecDelimit) {
    if (StringUtils.isBlank(strVec)) {
      return null;
    }
    if (StringUtils.isBlank(vecDelimit)) {
      return null;
    }
    String[] vect = strVec.split(vecDelimit, -1);
    return !"".equals(vect[vect.length - 1]) ? vect[vect.length - 1] : null;
  }

  public static String substring(String url, String start, String end) {
    if (org.apache.commons.lang3.StringUtils.isBlank(url)) {
      return null;
    }
    int startPos;
    int endPos;

    if (!org.apache.commons.lang3.StringUtils.isBlank(start)) {
      startPos = url.indexOf(start);
      if (startPos < 0) {
        return null;
      } else {
        startPos += start.length();
        if (startPos == url.length())
          return null;
      }
    } else {
      startPos = 0;
    }

    if (org.apache.commons.lang3.StringUtils.isBlank(end)) {
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
