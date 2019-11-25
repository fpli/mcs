package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public interface RedirectStrategy {
  URI process(HttpServletRequest request, HttpServletResponse response, CookieReader cookie,
              ContainerRequestContext context) throws IOException, URISyntaxException;
}
