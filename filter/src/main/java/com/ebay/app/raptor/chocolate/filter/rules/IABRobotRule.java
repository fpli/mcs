package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.rules.uamatch.FourParamsListEntry;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.kernel.context.RuntimeContext;
import org.apache.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Blacklist-type rule for IAB robot detection
 *
 * Created by jialili1 on 2/27/19
 */
public class IABRobotRule extends BaseFilterRule {
  private List<FourParamsListEntry> blacklist = new ArrayList<FourParamsListEntry>();
  private String blacklistName;

  public IABRobotRule(ChannelType channelType) {
    super(channelType);
    this.readFromLocalFiles();
  }

  /**
   * Create an empty instance for unit tests
   *
   * @return new empty instance
   */
  public static IABRobotRule createForTest(ChannelType channelType) {
    return new IABRobotRule(channelType);
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
   * @param uaString user agent string to match
   * @return true if the user agent string passes the test
   */
  private boolean isUserAgentValid(String uaString) {
    boolean result = true;

    if (uaString == null)
      return false;

    for (FourParamsListEntry entry : this.blacklist) {
      if (entry.match(uaString)) {
        result = false;
        break;
      }
    }

    return result;
  }

  /**
   * Test the user agent from the request using the IAB blacklist
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
      blacklistName = this.filterRuleContent.getBlacklistName();
      String blString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + blacklistName)));
      this.readFromStrings(blString);
    } catch (Exception e) {
      Logger.getLogger(IABRobotRule.class).error("Failed to get IAB Spiders and Robots list", e);
      throw new Error("IAB Spiders and Robots List not found", e);
    }
  }
}
