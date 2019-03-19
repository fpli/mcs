package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.ZooKeeperConnection;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.rules.uamatch.FourParamsListEntry;
import com.ebay.app.raptor.chocolate.filter.rules.uamatch.TwoParamsListEntry;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.kernel.context.RuntimeContext;
import org.apache.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a two-pass IAB test using a whitelist and a blacklist
 * To pass this rule, the event (impression/click) must match one of the whitelist entries,
 * while not matching any of the blacklist entries (unless it matches an exception in the same blacklist rule)
 * <p>
 * Created by spugach on 11/17/16.
 */
public class TwoPassIABRule extends BaseFilterRule {
  public static final String FILTERING_IAB_WHITELIST_ZK_PATH = "chocolate.filter.iabtest.whitelist";
  public static final String FILTERING_IAB_BLACKLIST_ZK_PATH = "chocolate.filter.iabtest.blacklist";
  private String whitelistName;
  private String blacklistName;
  private List<TwoParamsListEntry> whitelist = new ArrayList<TwoParamsListEntry>();
  private List<FourParamsListEntry> blacklist = new ArrayList<FourParamsListEntry>();
  
  public TwoPassIABRule(ChannelType channelType) {
    super(channelType);
    this.createFromOptions();
  }
  
  /**
   * Create an empty instance for unit tests
   *
   * @return new empty instance
   */
  public static TwoPassIABRule createForTest(ChannelType channelType) {
    return new TwoPassIABRule(channelType);
  }
  
  /**
   * For unit test purposes: clear the lists
   */
  public void clear() {
    this.whitelist.clear();
    this.blacklist.clear();
  }
  
  /**
   * Add a whitelist entry
   *
   * @param whitelistEntry IAB-format whitelist string
   */
  public void addWhitelistEntry(String whitelistEntry) {
    this.whitelist.add(new TwoParamsListEntry(whitelistEntry));
  }
  
  /**
   * Add a blacklist entry
   *
   * @param blacklistEntry IAB-format blacklist string
   */
  public void addBlacklistEntry(String blacklistEntry) {
    this.blacklist.add(new FourParamsListEntry(blacklistEntry));
  }
  
  /**
   * Reset the lists from the strings of the IAB file format:
   * - multiline
   * - # comments
   *
   * @param whitelist
   * @param blacklist
   */
  public void readFromStrings(String whitelist, String blacklist) {
    this.clear();
    
    String[] parts = whitelist.split("\n");
    for (String part : parts) {
      String t = part.trim();
      if (t.isEmpty() || t.startsWith("#")) {
        continue;
      }
      
      this.addWhitelistEntry(t);
    }
    
    parts = blacklist.split("\n");
    for (String part : parts) {
      String t = part.trim();
      if (t.isEmpty() || t.startsWith("#")) {
        continue;
      }
      
      this.addBlacklistEntry(t);
    }
  }
  
  /**
   * Match user agent string against the whitelist, then the blacklist
   *
   * @param uaString user agent string to match
   * @return true if the user agent string passes the test
   */
  private boolean isUserAgentValid(String uaString) {
    boolean result = false;
    for (TwoParamsListEntry entry : this.whitelist) {
      if (entry.match(uaString)) {
        result = true;
        break;
      }
    }
    
    if (!result) {
      return false;
    }
    
    for (FourParamsListEntry entry : this.blacklist) {
      if (entry.match(uaString)) {
        result = false;
        break;
      }
    }
    
    return result;
  }
  
  /**
   * Test the user agent from the request using the IAB two-pass rule (whitelist+blacklist)
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) { return isUserAgentValid(event.getUserAgent()) ? 0 : 1;
  }
  
  /**
   * Create a rule instance, and fill it with blacklist/whitelist entries based on the options
   *
   * @return new instance
   */
  public void createFromOptions() {
    if (!this.readFromLocalFiles()) {
      ZooKeeperConnection connection = new ZooKeeperConnection();
      try {
        connection.connect(ApplicationOptions.getInstance().getZookeeperString());
        String wlString = connection.getStringData(ApplicationOptions.getInstance().getByNameString(FILTERING_IAB_WHITELIST_ZK_PATH));
        String blString = connection.getStringData(ApplicationOptions.getInstance().getByNameString(FILTERING_IAB_BLACKLIST_ZK_PATH));
        this.readFromStrings(wlString, blString);
        connection.close();
      } catch (Exception e) {
        Logger.getLogger(TwoPassIABRule.class).error("Failed to get IAB lists", e);
        throw new Error("IAB Lists not found", e);
      }
    }
  }

  private boolean readFromLocalFiles() {
    try {
      whitelistName = this.filterRuleContent.getWhitelistName();
      blacklistName = this.filterRuleContent.getBlacklistName();
      String wlString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + whitelistName)));
      String blString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + blacklistName)));
      this.readFromStrings(wlString, blString);
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
