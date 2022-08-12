package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.flink.nrt.filter.configs.FilterRuleContent;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRequest;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Flag rule to judge if the referring URL is an eBay URL.
 * <p>
 * Created by jialili1 on 12/25/18
 */
public class EBayRefererDomainRule extends GenericBlacklistRule {
  private static String listName;
  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?[\\w-.]+\\.(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/.*)", Pattern.CASE_INSENSITIVE);

  public EBayRefererDomainRule(ChannelType channelType) {
    super(channelType);
    this.readFromLocalFile();
  }

  private void readFromLocalFile() {
    try {
      getBlacklistName(this.filterRuleContent);
      String blString = new String(PropertyMgr.getInstance().loadBytes(listName));
      this.readFromString(blString);
    } catch (Exception e) {
      Logger.getLogger(EBayRefererDomainRule.class).error("Failed to get eBay referer domain list in local file", e);
      throw new Error("eBay referer domain list not found", e);
    }
  }

  private static void getBlacklistName(FilterRuleContent filterRuleContent) {
    listName = filterRuleContent.getBlacklistName();
  }

  @Override
  public void readFromString(String blacklist) {
    this.clear();

    String[] parts = blacklist.split("[\n\r]");
    for (String part : parts) {
      String t = part.trim();
      if (t.isEmpty() || t.startsWith("#")) {
        continue;
      }

      this.add(t.toLowerCase());
    }
  }

  @Override
  protected String getFilteredValue(FilterRequest request) {
    return request.getReferrerDomain();
  }

  @Override
  public int test(FilterRequest request) {
    String refererDomain = getFilteredValue(request);
    if (refererDomain != null) {
      Matcher m = ebaysites.matcher(refererDomain);
      if (m.find()) {
        return 1;
      } else if (this.contains(getFilteredValue(request).toLowerCase())) {
        return 1;
      } else
        return 0;
    } else
      return 0;
  }
}
