package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.AdserviceResourceTest;
import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.LocalCacheIdMapping;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import org.checkerframework.checker.units.qual.C;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.ResponseCookie;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;

import java.io.IOException;
import java.util.UUID;

import static com.ebay.app.raptor.chocolate.adservice.constant.Constants.ADGUID;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class AdserviceCookieTest {

  private Boolean initialized = false;

  @Before
  public void setUp() throws IOException {
    if (!initialized) {
      RuntimeContext.setConfigRoot(AdserviceResourceTest.class.getClassLoader().getResource
              ("META-INF/configuration/Dev/"));
      ApplicationOptions.init();
      initialized = true;
    }
  }


  @Test
  public void setAdguidTestHaveCookieInRequestAllIsCorrect() {
    String cookieGuid = UUID.fromString("b3bb72cb-dd9c-4ae8-8aae-2ad51705db88")
            .toString()
            .replaceAll("-", "");

    MockHttpServletRequest req = new MockHttpServletRequest();
    MockHttpServletResponse res = new MockHttpServletResponse();

    Cookie cookie = new Cookie(ADGUID, cookieGuid);
    cookie.setPath("/");
    cookie.setSecure(true);
    cookie.setHttpOnly(true);
    cookie.setMaxAge(60);
    req.setCookies(cookie);

    String cookieVal = new AdserviceCookie().setAdguid(req, res);
    Cookie resCookie = res.getCookie(ADGUID);

    assertEquals(cookieGuid, cookieVal);
    assertNull(resCookie);
  }

  @Test
  public void setAdguidTestHaveCookieInRequestWrongPath() {
    String cookieGuid = UUID.fromString("b3bb72cb-dd9c-4ae8-8aae-2ad51705db88")
            .toString()
            .replaceAll("-", "");

    MockHttpServletRequest req = new MockHttpServletRequest();
    MockHttpServletResponse res = new MockHttpServletResponse();

    Cookie cookie = new Cookie(ADGUID, cookieGuid);
    cookie.setPath("somepath");
    cookie.setSecure(true);
    cookie.setHttpOnly(true);
    cookie.setMaxAge(60);
    req.setCookies(cookie);

    String cookieVal = new AdserviceCookie().setAdguid(req, res);
    Cookie resCookie = res.getCookie(ADGUID);

    assertEquals(cookieGuid, cookieVal);
    assertNotNull(resCookie);
    assertEquals(resCookie.getPath(), "/");
  }

  @Test
  public void setAdguidTestHaveCookieInRequestNoPath() {
    String cookieGuid = UUID.fromString("b3bb72cb-dd9c-4ae8-8aae-2ad51705db88")
            .toString()
            .replaceAll("-", "");

    MockHttpServletRequest req = new MockHttpServletRequest();
    MockHttpServletResponse res = new MockHttpServletResponse();

    Cookie cookie = new Cookie(ADGUID, cookieGuid);
    cookie.setSecure(true);
    cookie.setHttpOnly(true);
    cookie.setMaxAge(60);
    req.setCookies(cookie);

    String cookieVal = new AdserviceCookie().setAdguid(req, res);
    Cookie resCookie = res.getCookie(ADGUID);

    assertEquals(cookieGuid, cookieVal);
    assertNotNull(resCookie);
    assertEquals(resCookie.getPath(), "/");
  }

  @Test
  public void setAdguidTestDontHaveCookie() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    MockHttpServletResponse res = new MockHttpServletResponse();

    String cookieVal = new AdserviceCookie().setAdguid(req, res);
    Cookie resCookie = res.getCookie(ADGUID);

    assertNotNull(cookieVal);
    assertNotNull(resCookie);
    assertEquals(resCookie.getPath(), "/");
  }
}
