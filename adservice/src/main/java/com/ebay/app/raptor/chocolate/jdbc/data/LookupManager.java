package com.ebay.app.raptor.chocolate.jdbc.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class LookupManager {
  private static final Logger logger = LoggerFactory.getLogger(LookupManager.class);

  public static boolean isApprovedOffEbayDestination(final String destination) {
    final String destHost = getHostFromUrl(destination);
    if (destHost == null || destHost.length() == 0) {
      if (lookupProtocolWhitelist(destination)) {
        return true;
      }

      return false;
    }

    // Check against exception suffixes
    if (lookupFullWhitelist(destHost)) {
      return true;
    }

    // Check against exception hosts
    if (lookupPartialWhitelist(destHost)) {
      return true;
    }

    return false;
  }

  private static String getHostFromUrl(final String destination) {
    if (destination == null || destination.length() == 0) {
      return null;
    }

    // Parse the destination into a URL
    URL destUrl;
    try {
      destUrl = new URL(destination);
    } catch (Exception e) {
      logger.warn("Error in parsing destination into url: ", e);
      return null;
    }

    // Strip off host and validate that it is not empty or null
    return destUrl.getHost().toLowerCase().trim();
  }

  private static boolean lookupProtocolWhitelist(String destination) {
    return ThirdpartyWhitelistCache.getInstance().isInProtocolWhitelist(destination);
  }

  private static boolean lookupFullWhitelist(String destination) {
    return ThirdpartyWhitelistCache.getInstance().isInFullWhitelist(destination);
  }

  private static boolean lookupPartialWhitelist(String destination) {
    return ThirdpartyWhitelistCache.getInstance().isInParitialWhitelist(destination);
  }

}
