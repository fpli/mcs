package com.ebay.app.raptor.chocolate.adservice.redirect;

import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.container.ContainerRequestContext;
import java.net.URI;
import java.net.URISyntaxException;

public interface RedirectStrategy {
  /**
   * Handle the redirection request, get redirection url and call mcs to send ubi event if needed
   * @param request   http request
   * @param mktClient http client to call MCS
   * @param endpoint  MCS endpoint
   * @param guid      Cookie guid
   * @param adguid    Cookie adguid
   * @return  redirection url
   * @throws URISyntaxException
   */
  URI process(HttpServletRequest request, Client mktClient, String endpoint, String guid, String adguid)
      throws URISyntaxException;

  /**
   * Get redirection url
   * @param parameters  request url parameters
   */
  void generateRedirectUrl(MultiValueMap<String, String> parameters);
}
