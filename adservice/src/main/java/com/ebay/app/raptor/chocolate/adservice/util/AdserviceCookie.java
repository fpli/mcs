package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final String METRIC_READ_ADGUID = "METRIC_READ_ADGUID";
  private static final String METRIC_NO_ADGUID_IN_COOKIE = "METRIC_NO_ADGUID_IN_COOKIE";
  private static final String METRIC_HAS_ADGUID_IN_COOKIE = "METRIC_HAS_ADGUID_IN_COOKIE";
  private static final String METRIC_SET_NEW_ADGUID = "METRIC_SET_NEW_ADGUID";

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
    metrics.meter(METRIC_READ_ADGUID);
    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (Cookie cookie :
          cookies) {
        if(cookie.getName().equalsIgnoreCase(ADGUID)) {
          metrics.meter(METRIC_HAS_ADGUID_IN_COOKIE);
          return cookie.getValue();
        }
      }
    }
    metrics.meter(METRIC_NO_ADGUID_IN_COOKIE);
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
      metrics.meter(METRIC_SET_NEW_ADGUID);
      adguid = UUID.randomUUID().toString();

      ResponseCookie cookie = ResponseCookie.from(ADGUID, adguid)
          .maxAge(COOKIE_EXPIRY)
          .path("/")
          .sameSite("None")
          .secure(true)
          .build();

      response.addHeader("Set-Cookie", cookie.toString());
    }
    return adguid;
  }
}
