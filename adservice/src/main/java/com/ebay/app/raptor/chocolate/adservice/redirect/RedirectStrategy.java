package com.ebay.app.raptor.chocolate.adservice.redirect;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public interface RedirectStrategy {
  void process(HttpServletRequest request, HttpServletResponse response) throws IOException;
  String generateRedirectUrl(HttpServletRequest request);
  boolean isValidRedirectUrl(String redirectUrl);
}
