package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.ZooKeeperConnection;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.kernel.context.RuntimeContext;
import org.apache.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Blacklist-type rule for referrer domains
 * <p>
 * Created by spugach on 11/30/16.
 */
public class EPNDomainBlacklistRule extends GenericBlacklistRule {
  public static final String FILTERING_EPN_DOMAIN_LIST_ZK_PATH = "chocolate.filter.epnblacklist.domains";
  /**
   * serialVersionUID required in a serializable class
   */
  private static final long serialVersionUID = -3119973586077733944L;
  private static final String EPN_BLACKLIST_FILENAME = "EPN_domains_Blacklist.txt";
  
  public EPNDomainBlacklistRule(ChannelType channelType) {
    super(channelType);
    this.createFromOptions();
  }
  
  /**
   * For unit testing
   */
  public static EPNDomainBlacklistRule createForTest(ChannelType channelType) { return new EPNDomainBlacklistRule(channelType); }
  
  /**
   * Create from blacklist stored in ZK
   *
   * @return new instance
   */
  public void createFromOptions() {
    if (!this.readFromLocalFile()) {
      ZooKeeperConnection connection = new ZooKeeperConnection();
      try {
        connection.connect(ApplicationOptions.getInstance().getZookeeperString());
        String blacklistString = connection.getStringData(ApplicationOptions.getInstance().getByNameString(FILTERING_EPN_DOMAIN_LIST_ZK_PATH));
        this.readFromString(blacklistString);
        connection.close();
      } catch (Exception e) {
        Logger.getLogger(EPNDomainBlacklistRule.class).error("Failed to get domain blacklist", e);
        throw new Error("Domain blackist not found", e);
      }
    }
  }
  
  private boolean readFromLocalFile() {
    try {
      String blString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + EPN_BLACKLIST_FILENAME)));
      this.readFromString(blString);
    } catch (Exception e) {
      return false;
    }
    
    return true;
  }
  
  @Override
  protected String getFilteredValue(FilterRequest request) {
    return request.getReferrerDomain();
  }
}
