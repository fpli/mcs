package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseCookie;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.UUID;

/**
 * Read and write ebayadserving.com cookie. The purpose of this cookie is for synchronizing adguid with ebay.com guid.
 * When user comes to ebayadserving.com, the server will fetch adguid in cookie and then find corresponding guid.
 * With the information of guid, call DAP to do personalization.
 * @author xiangli4
 * @since 2019/12/11
 */
@Component
public class AdserviceCookie {
  private static final Logger logger = LoggerFactory.getLogger(AdserviceCookie.class);
  private Metrics metrics;
  private static final String ADGUID = "adguid";
  // expires in 90 days
  private static final int COOKIE_EXPIRY = 90 * 24 * 60 * 60;
  private static final String DEFAULT_ADGUID = "00000000000000000000000000000000";
  private static final String METRIC_READ_ADGUID = "METRIC_READ_ADGUID";
  private static final String METRIC_NO_ADGUID_IN_COOKIE = "METRIC_NO_ADGUID_IN_COOKIE";
  private static final String METRIC_HAS_ADGUID_IN_COOKIE = "METRIC_HAS_ADGUID_IN_COOKIE";
  private static final String METRIC_SET_NEW_ADGUID = "METRIC_SET_NEW_ADGUID";
  private static final String METRIC_ERROR_CREATE_ADGUID = "METRIC_ERROR_CREATE_ADGUID";

  @Autowired
  @Qualifier("cb")
  private IdMapable idMapping;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }
  /**
   * Read adguid from cookie
   * @param request http request
   * @return adguid in String
   */
  public String readAdguid(HttpServletRequest request) {
    ESMetrics.getInstance().meter(METRIC_READ_ADGUID);
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie cookie :
          cookies) {
        if(cookie.getName().equalsIgnoreCase(ADGUID)) {
          ESMetrics.getInstance().meter(METRIC_HAS_ADGUID_IN_COOKIE);
          return cookie.getValue();
        }
      }
    }
    ESMetrics.getInstance().meter(METRIC_NO_ADGUID_IN_COOKIE);
    return null;
  }

  /**
   * Set adguid in cookie if there is no
   * @param request http request
   * @param response http response
   * @return adguid in String
   */
  public String setAdguid(HttpServletRequest request, HttpServletResponse response) {
    String adguid = readAdguid(request);
    if(adguid == null) {
      ESMetrics.getInstance().meter(METRIC_SET_NEW_ADGUID);
      try {
        // same format as current guid, 32 digits
        adguid = UUID.randomUUID().toString().replaceAll("-","");
      } catch(Exception e) {
        ESMetrics.getInstance().meter(METRIC_ERROR_CREATE_ADGUID);
        logger.warn(e.toString());
        adguid = DEFAULT_ADGUID;
      }

      ResponseCookie cookie;
      if(ApplicationOptions.getInstance().isSecureCookie()) {
        cookie = ResponseCookie.from(ADGUID, adguid)
            .maxAge(COOKIE_EXPIRY)
            .sameSite("None")
            .httpOnly(true)
            .secure(true)
            .build();
      } else {
        cookie = ResponseCookie.from(ADGUID, adguid)
            .maxAge(COOKIE_EXPIRY)
            .sameSite("None")
            .httpOnly(true)
            .build();
      }

      response.addHeader("Set-Cookie", cookie.toString());
    }
    return adguid;
  }


  /**
   * Get guid from mapping
   * @param request http request
   * @return guid
   */
  public String getGuid(HttpServletRequest request) {
    String adguid = readAdguid(request);
    String guid = idMapping.getGuid(adguid);
    if(StringUtils.isEmpty(guid)) {
      guid = "";
    }
    return guid;
  }

  /**
   * Get user id from mapping
   * @param request http request
   * @return decrypted user id
   */
  public String getUserId(HttpServletRequest request) {
    String adguid = readAdguid(request);
    String encryptedUserid = idMapping.getUid(adguid);
    if(StringUtils.isEmpty(encryptedUserid)) {
      encryptedUserid = "0";
    }
    return String.valueOf(decryptUserId(encryptedUserid));
  }

  /**
   * Decrypt user id from encrypted user id
   * @param encryptedStr encrypted user id
   * @return actual user id
   */
  private long decryptUserId(String encryptedStr) {
    long xorConst = 43188348269L;

    long encrypted = 0;

    try {
      encrypted = Long.parseLong(encryptedStr);
    }
    catch (NumberFormatException e) {
      return -1;
    }

    long decrypted = 0;

    if(encrypted > 0){
      decrypted  = encrypted ^ xorConst;
    }

    return decrypted;
  }
}
