package com.ebay.app.raptor.chocolate.adservice.redirect;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
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

  /**
   * Generate the redirect URL
   */
  @Override
  public void generateRedirectUrl(MultiValueMap<String, String> parameters) {
    redirectionEvent.setRedirectSource("default");
    redirectionEvent.setRedirectUrl(ApplicationOptions.getInstance().getRedirectHomepage());
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
