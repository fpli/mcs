package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.kernel.context.RuntimeContext;
import org.apache.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;

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
  public void createFromBundledFile() {
    try {
      blacklistName = this.filterRuleContent.getBlacklistName();
      String blString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + blacklistName)));
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
