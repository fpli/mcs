package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.ZooKeeperConnection;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.rules.uamatch.BlacklistEntry;
import com.ebay.app.raptor.chocolate.filter.rules.uamatch.WhitelistEntry;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterWeightedRule;
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
public class TwoPassIABRule extends BaseFilterWeightedRule {
  public static final String FILTERING_IAB_WHITELIST_ZK_PATH = "chocolate.filter.iabtest.whitelist";
  public static final String FILTERING_IAB_BLACKLIST_ZK_PATH = "chocolate.filter.iabtest.blacklist";
  private static final String IAB_WHITELIST_FILENAME = "IAB_ABC_International_List_of_Valid_Browsers.txt";
  private static final String IAB_BLACKLIST_FILENAME = "IAB_ABC_International_Spiders_and_Robots.txt";
  private List<WhitelistEntry> whitelist = new ArrayList<WhitelistEntry>();
  private List<BlacklistEntry> blacklist = new ArrayList<BlacklistEntry>();
  
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
    this.whitelist.add(new WhitelistEntry(whitelistEntry));
  }
  
  /**
   * Add a blacklist entry
   *
   * @param blacklistEntry IAB-format blacklist string
   */
  public void addBlacklistEntry(String blacklistEntry) {
    this.blacklist.add(new BlacklistEntry(blacklistEntry));
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
   * @param uaString uaer agent string to match
   * @return true if the user agent string passes the test
   */
  private boolean isUserAgentValid(String uaString) {
    boolean result = false;
    for (WhitelistEntry entry : this.whitelist) {
      if (entry.match(uaString)) {
        result = true;
        break;
      }
    }
    
    if (!result) {
      return false;
    }
    
    for (BlacklistEntry entry : this.blacklist) {
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
   * @return fail weight
   */
  @Override
  public float test(FilterRequest event) {
    return isUserAgentValid(event.getUserAgent()) ? 0 : getRuleWeight();
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
      String wlString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + IAB_WHITELIST_FILENAME)));
      String blString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + IAB_BLACKLIST_FILENAME)));
      this.readFromStrings(wlString, blString);
    } catch (Exception e) {
      return false;
    }
    
    return true;
  }
}
