package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.kernel.presentation.constants.PresentationConstants;

import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class PulsarParseUtils {

  /**
   * Get channel id from urlQueryString. Compatible with DSS SQL:
   * when soj_isint(soj_list_last_element(soj_list_get_val_by_idx(urlquerystring,'\\\\?',1),'/'))=1
   * then soj_list_last_element(soj_list_get_val_by_idx(urlquerystring,'\\\\?',1),'/')
   * @param urlQueryString urlQueryString
   * @return channel id
   */
  public static String getChannelIdFromUrlQueryString(String urlQueryString) {
    if (StringUtils.isEmpty(urlQueryString)) {
      return null;
    }
    String evaluate = getValueByIndex(urlQueryString, "\\?", 1);
    return getLastElement(evaluate, "/");
  }

  public static String getPartnerIdFromUrlQueryString(String urlQueryString) {
    if (StringUtils.isEmpty(urlQueryString)) {
      return null;
    }
    String evaluate = getValueByIndex(urlQueryString, "\\?", 1);
    return getValueByIndex(evaluate, "/", 3);
  }

  /**
   * Get value from string. Compatible with com.ebay.sojourner.common.sojlib.SOJListGetValueByIndex
   * @param strVec string
   * @param vecDelimit delimiter
   * @param vecIdx index
   * @return value
   */
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
    if (vecIdx < vect.length) {
      return vect[vecIdx];
    } else {
      return null;
    }
  }

  /**
   * Return the last element of the string. Compatible with com.ebay.hadoop.udf.soj.SojListLastElement
   * @param strVec string
   * @param vecDelimit delimiter
   * @return last element
   */
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

  /**
   * Get the substring between start and end. Compatible with com.ebay.hadoop.udf.soj.StrBetweenEndList
   * @param url source string
   * @param start start string
   * @param end end string
   * @return substring
   */
  public static String substring(String url, String start, String end) {
    if (StringUtils.isBlank(url)) {
      return null;
    }
    int startPos;
    int endPos;

    if (!StringUtils.isBlank(start)) {
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

    if (StringUtils.isBlank(end)) {
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
   * Get soj tags from urlQueryString
   * @param urlQueryString urlQueryString
   * @return soj tags
   */
  public static Map<String, String> getSojTagsFromUrlQueryString(String urlQueryString) {
    Map<String, String> sojTagsMap = new HashMap<>();
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(urlQueryString).build();
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (!parameters.containsKey(TransformerConstants.SOJ_TAGS)) {
      return sojTagsMap;
    }
    if (parameters.get(TransformerConstants.SOJ_TAGS).get(0) == null) {
      return sojTagsMap;
    }

    String sojTags = parameters.get(TransformerConstants.SOJ_TAGS).get(0);
    try {
      sojTags = URLDecoder.decode(sojTags, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException | IllegalArgumentException e) {
      return sojTagsMap;
    }
    if (StringUtils.isEmpty(sojTags)) {
      return sojTagsMap;
    }
    StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
    while (stToken.hasMoreTokens()) {
      StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
      if (sojNvp.countTokens() == 2) {
        String sojTag = sojNvp.nextToken().trim();
        String urlParam = sojNvp.nextToken().trim();
        if (StringUtils.isNotEmpty(urlParam) && StringUtils.isNotEmpty(sojTag) && parameters.containsKey(urlParam)
                && parameters.getFirst(urlParam) != null) {
          sojTagsMap.put(sojTag, parameters.getFirst(urlParam));
        }
      }
    }
    return sojTagsMap;
  }
}
