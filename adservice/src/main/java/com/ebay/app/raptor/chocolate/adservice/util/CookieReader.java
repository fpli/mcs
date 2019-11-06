package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.raptor.cookie.api.RequestCookieData;
import org.springframework.stereotype.Component;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * @author xiangli4
 * Read ebay cookie
 */
@Component
public class CookieReader {

  private String getCookieletValue(ContainerRequestContext requestContext, int cookieletId) {
    //cookie
    RequestCookieData requestCookieData = (RequestCookieData) requestContext
        .getProperty(RequestCookieData.NAME);
    // read the information from ebay cookies
    String value = "";
    if (requestCookieData != null) {
      value = requestCookieData.getCookieletValue(cookieletId);
    }
    return value;

  }

  //CGUID cookielet id = 347
  public String getCguid(ContainerRequestContext requestContext) {
    return getCookieletValue(requestContext, 347);
  }

  //CGUID cookielet id = 315
  public String getGuid(ContainerRequestContext requestContext) {
    return getCookieletValue(requestContext, 315);

  }

}
