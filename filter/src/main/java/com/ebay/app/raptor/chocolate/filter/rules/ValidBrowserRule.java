package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
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
 * Whitelist-type rule for IAB valid browser detection
 *
 * Created by jialili1 on 2/27/19
 */
public class ValidBrowserRule extends BaseFilterRule {
  private List<TwoParamsListEntry> whitelist = new ArrayList<TwoParamsListEntry>();
  private String whitelistName;

  public ValidBrowserRule(ChannelType channelType) {
    super(channelType);
    this.readFromLocalFiles();
  }

  /**
   * Create an empty instance for unit tests
   *
   * @return new empty instance
   */
  public static ValidBrowserRule createForTest(ChannelType channelType) {
    return new ValidBrowserRule(channelType);
  }

  /**
   * For unit test purposes: clear the lists
   */
  public void clear() {
    this.whitelist.clear();
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
   * Reset the lists from the strings of the IAB file format:
   * - multiline
   * - # comments
   *
   * @param whitelist
   */
  public void readFromStrings(String whitelist) {
    this.clear();

    String[] parts = whitelist.split("\n");
    for (String part : parts) {
      String t = part.trim();
      if (t.isEmpty() || t.startsWith("#")) {
        continue;
      }

      this.addWhitelistEntry(t);
    }
  }

  /**
   * Match user agent string in the whitelist
   *
   * @param uaString user agent string to match
   * @return true if the user agent string passes the test
   */
  private boolean isUserAgentValid(String uaString) {
    boolean result = false;

    if (uaString == null)
      return false;

    for (TwoParamsListEntry entry : this.whitelist) {
      if (entry.match(uaString)) {
        result = true;
        break;
      }
    }

    return result;
  }

  /**
   * Test the user agent from the request using the IAB whitelist
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
      whitelistName = this.filterRuleContent.getWhitelistName();
      String wlString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + whitelistName)));
      this.readFromStrings(wlString);
    } catch (Exception e) {
      Logger.getLogger(ValidBrowserRule.class).error("Failed to get IAB Valid Browsers list", e);
      throw new Error("IAB Valid Browsers list not found", e);
    }
  }

}
