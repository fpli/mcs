package com.ebay.app.raptor.chocolate.jdbc.data;

import com.ebay.app.raptor.chocolate.jdbc.model.ThirdpartyWhitelist;
import com.ebay.app.raptor.chocolate.jdbc.repo.ThirdpartyWhitelistRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Refresh the table and provide whitelist lookup
 * Created by jialili1 on 11/15/19
 */
public class ThirdpartyWhitelistCache {
  private static final Logger logger = LoggerFactory.getLogger(ThirdpartyWhitelistCache.class);

  private ThirdpartyWhitelistRepo thirdpartyWhitelistRepo;

  private static final Pattern domainPattern = Pattern.compile("[a-z0-9-]+(\\.[a-z0-9-]+)+");

  /**
   Singleton instance of ThirdpartyWhitelistCache
   */
  private static ThirdpartyWhitelistCache INSTANCE = null;

  /**
   * The timer to refresh thirdparty whitelist
   */
  private Timer timer;

  /**
   * Refresh interval is 15 min
   */
  private static final long REFRESH_INTERVAL = 15 * 60 * 1000L;

  /**
   * Partial domain type id
   */
  private static final Integer PARTIAL_DOMAIN_TYPE_ID = 4;

  /**
   * Full domain type id
   */
  private static final Integer FULL_DOMAIN_TYPE_ID = 5;

  /**
   * Protocol suffix type id
   */
  private static final Integer PROTOCOL_SUFFIX_TYPE_ID = 6;

  /**
   * Full domain whitelist
   */
  private List<ThirdpartyWhitelist> fullWhitelist = new ArrayList<ThirdpartyWhitelist>();

  /**
   * Parital domain whitelist
   */
  private List<ThirdpartyWhitelist> partialWhitelist = new ArrayList<ThirdpartyWhitelist>();

  /**
   * Protocol suffix whitelist
   */
  private static List<ThirdpartyWhitelist> protocolWhitelist = new ArrayList<ThirdpartyWhitelist>();

  ThirdpartyWhitelistCache(ThirdpartyWhitelistRepo thirdpartyWhitelistRepo) {
    this.thirdpartyWhitelistRepo = thirdpartyWhitelistRepo;
    // Start the timer
    timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        logger.info("Start refreshing the thirdparty whitelist");
        refreshThirdpartyWhitelist();
      }
    }, 0, REFRESH_INTERVAL);
  }

  /**
   * Initialize the thirdparty whitelist cache and get the protocol whitelist
   */
  public static synchronized void init(ThirdpartyWhitelistRepo thirdpartyWhitelistRepo) {
    if (INSTANCE != null) {
      return;
    }
    INSTANCE = new ThirdpartyWhitelistCache(thirdpartyWhitelistRepo);
    protocolWhitelist = thirdpartyWhitelistRepo.findByTypeId(PROTOCOL_SUFFIX_TYPE_ID);
  }

  /**
   * @return the instance of ThirdpartyWhitelistCache
   */
  public static ThirdpartyWhitelistCache getInstance() {
    return INSTANCE;
  }

  /**
   * Refresh thirdparty whitelists
   */
  private void refreshThirdpartyWhitelist() {
    fullWhitelist = thirdpartyWhitelistRepo.findByTypeId(FULL_DOMAIN_TYPE_ID);
    partialWhitelist = thirdpartyWhitelistRepo.findByTypeId(PARTIAL_DOMAIN_TYPE_ID);
  }

  /**
   * Add this function for allowing non-http landing pages
   * For instance, the links which open an app could be tracked
   */
  public boolean isInProtocolWhitelist(String value) {
    if (value == null || value.trim().length() == 0) {
      return false;
    }

    int index = value.indexOf("://");
    if (index >= 0) {
      String protocol = value.substring(0, value.indexOf("://"));
      if (protocol != null) {
        protocol = protocol.toLowerCase();
      }

      Iterator<ThirdpartyWhitelist> iter = protocolWhitelist.iterator();
      while (iter.hasNext()) {
        String protocolSuffix = iter.next().getValue();
        if (protocolSuffix.equals(protocol)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Check the full domain list, domain must be exactly match
   */
  public boolean isInFullWhitelist(String value) {
    if (value == null || value.length() == 0) {
      return false;
    }

    Iterator<ThirdpartyWhitelist> iter = fullWhitelist.iterator();
    while (iter.hasNext()) {
      String domain = iter.next().getValue();
      if (value.equals(domain)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Check the partial domain list, domain should be end with partial domain
   */
  public boolean isInParitialWhitelist(String value) {
    if (value == null || value.length() == 0) {
      return false;
    }

    Iterator<ThirdpartyWhitelist> iter = partialWhitelist.iterator();
    while (iter.hasNext()) {
      String dest = iter.next().getValue();
      if (value.endsWith(dest) && isValidDomain(value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check the domain pattern to avoid redirect url spoofing with approved suffix
   */
  private static boolean isValidDomain(String in) {
    Matcher m = domainPattern.matcher(in);
    return m.matches();
  }

}
