package com.ebay.traffic.chocolate.mkttracksvc.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for parsing application options
 *
 * @author jialili
 */
public class DriverId {
  /**
   * Private logging instance
   */
  private static final Logger logger = Logger.getLogger(DriverId.class);

  /**
   * Pattern for a regex
   */
  private static final Pattern NUMBER_PATTERN = Pattern.compile("[0-9]+");

  /**
   * Refactored, host-indepedent code to derive a driver ID from the internet / hostname address.
   * <p>
   * Here's how it works:
   * <p>
   * 1. First, we get the largest number group in the FQDN. We mod that by 1024 and return that.
   * 2. If there is no number in the FQDN, we mod the decimal IP by 1024 and return that.
   *
   * @param hostname  to use as primary source
   * @param ipDecimal to fall back ono
   * @return the IP value.
   */
  public static int getDriverIdFromIp(String hostname, long ipDecimal) {
    Validate.isTrue(StringUtils.isNotBlank(hostname), "Hostname cannot be null at this point");
    Validate.isTrue(ipDecimal > 0l, "ipDecimal should be valid");

    // Get the largest number group in the FQDN
    long largest = -1l;
    Matcher matcher = NUMBER_PATTERN.matcher(hostname);
    while (matcher.find()) {
      String candidate = matcher.group();
      try {
        long entry = Long.parseLong(candidate);
        largest = Math.max(largest, entry);
      } catch (NumberFormatException e) {
        logger.warn("Could not parse string from hostname=" + hostname + " portion=" + candidate);
      }
    }

    // If we have a match, return the maximum value.
    long numeric = largest >= 0 ? largest : ipDecimal;
    int driverId = Long.valueOf(numeric % (SessionId.MAX_DRIVER_ID + 1l)).intValue();
    logger.info("Parsed driver ID from IP=" + driverId);
    return driverId;
  }

  /**
   * Utility function to derive a driver ID, first going from the longest numeric sequence
   * in the hostname, and falling back on the IP address.
   */
  public static int getDriverIdFromIp() {
    return getDriverIdFromIp(Hostname.HOSTNAME, Hostname.IP_DECIMAL);
  }
}