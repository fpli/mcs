package com.ebay.app.raptor.chocolate.adservice.redirect;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.container.ContainerRequestContext;
import java.net.URI;
import java.net.URISyntaxException;

public class RedirectContext {
  private RedirectStrategy strategy;

  public RedirectContext(RedirectStrategy strategy){
    this.strategy = strategy;
  }

  public URI execute(HttpServletRequest request, ContainerRequestContext context, Client mktClient, String endpoint)
      throws URISyntaxException {
    return strategy.process(request, context, mktClient, endpoint);
  }
}