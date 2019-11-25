package com.ebay.app.raptor.chocolate.adservice.redirect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

/**
 * For common thirdparty redirection cases
 *
 * Created by jialili1 on 11/18/19
 */
public class ThirdpartyRedirectStrategy extends BaseRedirectStrategy {
  private static final Logger logger = LoggerFactory.getLogger(ThirdpartyRedirectStrategy.class);

  private static final String DEFAULT_REDIRECT_URL = "http://www.ebay.com";


  /**
   * Generate the redirect URL
   */
  @Override
  protected void generateRedirectUrl(MultiValueMap<String, String> parameters) {
    redirectionEvent.setRedirectSource("default");
    redirectionEvent.setRedirectUrl(DEFAULT_REDIRECT_URL);
    // get loc parameter from request, if loc URL is valid, return it as redirect url
    // get loc URL
    String targetLocation = getTargetLocation(parameters);
    // if loc URL is valid, return it as redirect url
    if (isValidRedirectUrl(targetLocation)) {
      redirectionEvent.setRedirectSource("loc");
      redirectionEvent.setRedirectUrl(targetLocation);
    }
  }

}
