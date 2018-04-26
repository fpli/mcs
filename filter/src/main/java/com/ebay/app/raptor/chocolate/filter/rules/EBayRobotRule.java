package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.ZooKeeperConnection;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
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
 * Blacklist-type rule for eBay robots detection.
 *
 * Created by jialili1 on 4/23/18.
 */
public class EBayRobotRule extends BaseFilterRule {
  private List<TwoParamsListEntry> blacklist = new ArrayList<TwoParamsListEntry>();
  private String listName = null;

  public EBayRobotRule(ChannelType channelType) {
    super(channelType);
    this.readFromLocalFiles();
  }

  /**
   * Create an empty instance for unit tests
   *
   * @return new empty instance
   */
  public static EBayRobotRule createForTest(ChannelType channelType) {
    return new EBayRobotRule(channelType);
  }

  /**
   * For unit test purposes: clear the lists
   */
  public void clear() {
    this.blacklist.clear();
  }

  /**
   * Add a blacklist entry
   *
   * @param blacklistEntry eBay robot format blacklist string
   */
  public void addBlacklistEntry(String blacklistEntry) {
    this.blacklist.add(new TwoParamsListEntry(blacklistEntry));
  }

  /**
   * Reset the lists from the strings of the eBay robot file format:
   *
   * @param blacklist
   */
  public void readFromStrings(String blacklist) {
    this.clear();

    String[] parts = blacklist.split("\n");
    for (String part : parts) {
      String t = part.trim();
      if (t.isEmpty() || t.startsWith("#")) {
        continue;
      }

      this.addBlacklistEntry(t);
    }
  }

  /**
   * Match user agent string against the blacklist
   *
   * @param uaString uaer agent string to match
   * @return true if the user agent string passes the test
   */
  private boolean isUserAgentValid(String uaString) {
    boolean result = true;

    if (uaString == null)
      return false;

    for (TwoParamsListEntry entry : this.blacklist) {
      if (entry.match(uaString)) {
        result = false;
        break;
      }
    }

    return result;
  }

  /**
   * Test the user agent from the request using the EBayRobotRule
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    return isUserAgentValid(event.getUserAgent()) ? 0 : 1;
  }

  private void readFromLocalFiles() {
    try {
      listName = this.filterRuleContent.getListName();
      String blString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + listName)));
      this.readFromStrings(blString);
    } catch (Exception e) {
      Logger.getLogger(TwoPassIABRule.class).error("Failed to get eBay Spiders and Robots lists", e);
      throw new Error("eBay Spiders and Robots Lists not found", e);
    }
  }
}
