package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.flink.nrt.filter.configs.FilterRuleContent;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRequest;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.log4j.Logger;

/**
 * Blacklist-type rule for referrer domains
 * <p>
 * Created by spugach on 11/30/16.
 */
public class EPNDomainBlacklistRule extends GenericBlacklistRule {
  public static final String FILTERING_EPN_DOMAIN_LIST_ZK_PATH = "chocolate.filter.epnblacklist.domains";
  private static String blacklistName;
  /**
   * serialVersionUID required in a serializable class
   */
  private static final long serialVersionUID = -3119973586077733944L;

  public EPNDomainBlacklistRule(ChannelType channelType) {
    super(channelType);
    this.readFromLocalFile();
  }

  /**
   * For unit testing
   */
  public static EPNDomainBlacklistRule createForTest(ChannelType channelType) { return new EPNDomainBlacklistRule(channelType); }

  private void readFromLocalFile() {
    try {
      getBlacklistName(this.filterRuleContent);
      String blString = new String(PropertyMgr.getInstance().loadBytes(blacklistName));
      this.readFromString(blString);
    } catch (Exception e) {
      Logger.getLogger(EPNDomainBlacklistRule.class).error("Failed to get domain blacklist in local file", e);
      throw new Error("EPN domain blacklist not found", e);
    }
  }

  private static void getBlacklistName(FilterRuleContent filterRuleContent) {
    blacklistName = filterRuleContent.getBlacklistName();
  }

  @Override
  protected String getFilteredValue(FilterRequest request) {
    return request.getReferrerDomain();
  }
}
