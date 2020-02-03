package com.ebay.app.raptor.chocolate.adservice.redirect;

import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.net.URI;
import java.net.URISyntaxException;

public interface RedirectStrategy {
  /**
   * Handle the redirection request, get redirection url and call mcs to send ubi event if needed
   * @param request http request
   * @param context request context
   * @return        redirection url
   * @throws URISyntaxException
   */
  URI process(HttpServletRequest request, ContainerRequestContext context)
      throws URISyntaxException;

  /**
   * Get redirection url
   * @param parameters  request url parameters
   */
  void generateRedirectUrl(MultiValueMap<String, String> parameters);
}
