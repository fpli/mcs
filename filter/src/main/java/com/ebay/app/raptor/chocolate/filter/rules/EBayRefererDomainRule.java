package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.kernel.context.RuntimeContext;
import org.apache.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
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

  private boolean readFromLocalFile() {
    try {
      listName = this.filterRuleContent.getBlacklistName();
      String blString = new String(Files.readAllBytes(Paths.get(RuntimeContext.getConfigRoot().getFile() + listName)));
      this.readFromString(blString);
    } catch (Exception e) {
      Logger.getLogger(EBayRefererDomainRule.class).error("Failed to get eBay referer domain list in local file", e);
      return false;
    }

    return true;
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

      this.add(t.startsWith("www.") ? t.substring(4).toLowerCase() : t.toLowerCase());
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
