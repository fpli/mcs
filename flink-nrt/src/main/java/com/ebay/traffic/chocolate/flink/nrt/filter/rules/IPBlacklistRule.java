package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.flink.nrt.filter.configs.FilterRuleContent;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRequest;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.log4j.Logger;

/**
 * Blacklist events based on the IP address
 * <p>
 * Created by spugach on 11/30/16.
 */
public class IPBlacklistRule extends GenericBlacklistRule {
  private static String blacklistName;
  /**
   * required in a seralizable class
   */
  private static final long serialVersionUID = 1084924589653810527L;

  /**
   * Had to do this explicitly here, because Java doesn't have multiple inheritance
   *
   * @param channelType
   */
  public IPBlacklistRule(ChannelType channelType) {
    super(channelType);
    this.createFromBundledFile();
  }

  /**
   * Create from blacklist stored in ZK
   *
   * @return new instance
   */

  public static void init(FilterRuleContent filterRuleContent) {
    blacklistName = filterRuleContent.getBlacklistName();
  }

  public void createFromBundledFile() {
    try {
      init(this.filterRuleContent);
      String blString = new String(PropertyMgr.getInstance().loadBytes(blacklistName));
      this.readFromString(blString);
    } catch (Exception e) {
      Logger.getLogger(IPBlacklistRule.class).error("Failed to read IP blacklist");
      throw new Error("IP blacklist not found", e);
    }
  }

  @Override
  protected String getFilteredValue(FilterRequest request) {
    return request.getSourceIP();
  }
}
