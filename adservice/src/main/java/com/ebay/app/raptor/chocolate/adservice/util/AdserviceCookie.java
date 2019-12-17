package com.ebay.app.raptor.chocolate.adservice.util;
import org.springframework.stereotype.Component;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
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

  private static final String ADGUID = "adguid";
  // expires in 90 days
  private static final int COOKIE_EXPIRY = 90 * 24 * 60 * 60;
  private Map<String, String> AdguidGuidMap = new HashMap<>();

  /**
   * Read adguid from cookie
   * @param request http request
   * @return adguid in String
   */
  public String readAdguid(HttpServletRequest request) {
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie cookie :
          cookies) {
        if(cookie.getName().equalsIgnoreCase(ADGUID)) {
          return cookie.getValue();
        }
      }
    }
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
      adguid = UUID.randomUUID().toString();
      Cookie cookie = new Cookie(ADGUID, adguid);
      cookie.setMaxAge(COOKIE_EXPIRY);
      response.addCookie(cookie);
    }
    return adguid;
  }
}
