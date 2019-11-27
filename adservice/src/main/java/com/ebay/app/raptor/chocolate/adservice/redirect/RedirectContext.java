package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RedirectContext {
  private RedirectStrategy strategy;

  public RedirectContext(RedirectStrategy strategy){
    this.strategy = strategy;
  }

  public URI execute(HttpServletRequest request, CookieReader cookie, ContainerRequestContext context)
      throws IOException, URISyntaxException {
    return strategy.process(request, cookie, context);
  }
}
