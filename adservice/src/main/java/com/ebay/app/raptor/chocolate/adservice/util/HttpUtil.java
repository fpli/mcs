package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Map;

public class HttpUtil {
  private static final Logger logger = LoggerFactory.getLogger(HttpUtil.class);

  /**
   * Construct a tracking header.
   * @param guid    guid from mapping if there is
   * @param adguid  adguid from cookie
   * @return X-EBAY-C-TRACKING header value
   */
  public static String constructTrackingHeader(String guid, String adguid) {
    String cookie = "";
    if (!StringUtils.isEmpty(guid) && guid.length() >= Constants.GUID_LENGTH) {
      cookie += Constants.GUID + StringConstants.EQUAL + guid.substring(0, Constants.GUID_LENGTH);
    } else if (!StringUtils.isEmpty(guid)) {
      logger.warn("Invalid guid {}", guid);
    }

    if (!StringUtils.isEmpty(adguid)) {
      if (!StringUtils.isEmpty(cookie)) {
        cookie += StringConstants.COMMA;
      }
      cookie += Constants.ADGUID + StringConstants.EQUAL + adguid;
    }

    return cookie;
  }

  /**
   * Transform http parameters from map to multi value map
   * @param params  parameter map
   * @return  multi value parameter map
   */
  public static MultiValueMap<String, String> parse(Map<String, String[]> params) {
    MultiValueMap<String, String> result = new LinkedMultiValueMap<>();
    for (Map.Entry<String, String[]> param : params.entrySet()) {
      String[] values = param.getValue();
      for (String value : values) {
        result.add(param.getKey(), value);
      }
    }
    return result;
  }
}
