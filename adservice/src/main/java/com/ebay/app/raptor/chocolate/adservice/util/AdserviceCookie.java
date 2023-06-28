package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
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
  private static final String ADGUID = "adguid";
  private static final String ADGUID_COOKIE_PATH = "/";
  // expires in 90 days
  private static final int COOKIE_EXPIRY = 90 * 24 * 60 * 60;
  private static final String DEFAULT_ADGUID = "00000000000000000000000000000000";
  private static final String METRIC_READ_ADGUID = "METRIC_READ_ADGUID";
  private static final String METRIC_NO_ADGUID_IN_COOKIE = "METRIC_NO_ADGUID_IN_COOKIE";
  private static final String METRIC_HAS_ADGUID_IN_COOKIE = "METRIC_HAS_ADGUID_IN_COOKIE";
  private static final String METRIC_SET_NEW_ADGUID = "METRIC_SET_NEW_ADGUID";
  private static final String METRIC_ADGUID_COOKIE_PATH_CORRECTION = "METRIC_ADGUID_COOKIE_PATH_CORRECTION";
  private static final String METRIC_ERROR_CREATE_ADGUID = "METRIC_ERROR_CREATE_ADGUID";

  @Autowired
  @Qualifier("cb")
  private IdMapable idMapping;

  @PostConstruct
  public void postInit() {
  }

  /**
   * Read adguid from request cookie
   * @param request http request
   * @return adguid in String
   */
  public String readAdguid(HttpServletRequest request) {
    MonitorUtil.info(METRIC_READ_ADGUID);

    Cookie cookie = getAdguidCookie(request);
    if (cookie != null) {
      MonitorUtil.info(METRIC_HAS_ADGUID_IN_COOKIE);
      return cookie.getValue();
    }

    MonitorUtil.info(METRIC_NO_ADGUID_IN_COOKIE);
    return null;
  }

  public Cookie getAdguidCookie(HttpServletRequest request) {
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if(cookie.getName().equalsIgnoreCase(ADGUID)) {
          return cookie;
        }
      }
    }

    return null;
  }

  /**
   * Read adguid from request and response cookie
   * For first coming request, only response header has adguid
   * @param request   http request
   * @param response  http response
   * @return adguid in String
   */
  public String readAdguid(HttpServletRequest request, HttpServletResponse response) {
    String adguid = readAdguid(request);
    if (StringUtils.isEmpty(adguid)) {
      String cookie = response.getHeader("Set-Cookie");
      adguid = getCookie(cookie, Constants.ADGUID);
    }

    return adguid;
  }

  /**
   * Set adguid in cookie within response if there is no cookie presented in request
   * Makes sure the cookie Path is "/"
   *
   * @param request http request
   * @param response http response
   *
   * @return adguid in String
   */
  public String setAdguid(HttpServletRequest request, HttpServletResponse response) {
    Cookie adguidCookie = getAdguidCookie(request);

    // in case we don't have cookie - we generate new adguid
    // otherwise - get the value of the existing cookie
    String adguid = adguidCookie == null ?
            generateAdguid() :
            adguidCookie.getValue();

    ResponseCookie cookie = ResponseCookie.from(ADGUID, adguid)
            .maxAge(COOKIE_EXPIRY)
            .sameSite(org.springframework.boot.web.server.Cookie.SameSite.NONE.attributeValue())
            .path(ADGUID_COOKIE_PATH)
            .secure(ApplicationOptions.getInstance().isSecureCookie())
            .httpOnly(true)
            .build();

    response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());

    return adguid;
  }

  private static String generateAdguid() {
    String adguid;
    MonitorUtil.info(METRIC_SET_NEW_ADGUID);
    try {
      // same format as current guid, 32 digits
      adguid = UUID.randomUUID().toString().replaceAll("-", "");
    } catch (Exception e) {
      MonitorUtil.info(METRIC_ERROR_CREATE_ADGUID);
      logger.warn(e.toString());
      adguid = DEFAULT_ADGUID;
    }
    return adguid;
  }

  /**
   * Get guid list from mapping
   * @param request http request
   * @return guid list
   */
  public String getGuidList(HttpServletRequest request) {
    String adguid = readAdguid(request);
    String guidList = idMapping.getGuidListByAdguid(adguid);
    if(StringUtils.isEmpty(guidList)) {
      guidList = "";
    }
    return guidList;
  }

  /**
   * Get guid from mapping
   * @param request http request
   * @return guid
   */
  public String getGuid(HttpServletRequest request) {
    String adguid = readAdguid(request);
    String guid = idMapping.getGuidByAdguid(adguid);
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
    String encryptedUserid = idMapping.getUidByAdguid(adguid);
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

  /**
   * Get specific cookie from header
   * @param cookies cookie header
   * @param name    cookie name
   * @return cookie value
   */
  private String getCookie(String cookies, String name) {
    if (!StringUtils.isEmpty(cookies)) {
      String[] cookieArray = cookies.split(StringConstants.SEMICOLON);
      for (String cookie : cookieArray) {
        if (cookie.startsWith(name + StringConstants.EQUAL)) {
          return cookie.substring(cookie.indexOf(StringConstants.EQUAL) + 1);
        }
      }
    }

    return null;
  }
}
