package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.traffic.chocolate.flink.nrt.filter.FilterRuleMgr;
import com.ebay.traffic.chocolate.flink.nrt.filter.configs.FilterRuleContent;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.BaseFilterRule;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRequest;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by spugach don 11/30/16.
 */
public class BlacklistRulesTest {

  @BeforeClass
  public static void setUp() throws IOException {
    FilterRuleMgr.getInstance().initFilterRuleConfig("filter_rule_config.json");
  }

  @Before
  public void addTestRules() throws IOException {
    //Default Testing Data
    Map<ChannelType, Map<String, FilterRuleContent>> filterRules = FilterRuleMgr.getInstance().getFilterRuleConfigMap();

    //Testing Data for EPN channel
    filterRules.get(ChannelType.EPN).put(TwoPassIABRule.class.getSimpleName(), new FilterRuleContent
        (TwoPassIABRule.class.getSimpleName(), "IAB_ABC_International_List_of_Valid_Browsers.txt" , "IAB_ABC_International_Spiders_and_Robots.txt"));
    filterRules.get(ChannelType.EPN).put(TestBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (TestBlacklistRule.class.getSimpleName()));
    filterRules.get(ChannelType.EPN).put(IPBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (IPBlacklistRule.class.getSimpleName(), "IP_Blacklist_EPN.txt"));
    filterRules.get(ChannelType.EPN).put(EPNDomainBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (EPNDomainBlacklistRule.class.getSimpleName(), "EPN_domains_Blacklist.txt"));
    filterRules.get(ChannelType.EPN).put(EBayRobotRule.class.getSimpleName(), new FilterRuleContent
        (EBayRobotRule.class.getSimpleName(), "eBay_Spiders_and_Robots_EPN.txt"));
    filterRules.get(ChannelType.EPN).put(EBayRefererDomainRule.class.getSimpleName(), new FilterRuleContent
        (EBayRefererDomainRule.class.getSimpleName(), "eBay_Referral_Domain.txt"));
    filterRules.get(ChannelType.EPN).put(ValidBrowserRule.class.getSimpleName(), new FilterRuleContent
        (ValidBrowserRule.class.getSimpleName(), "IAB_ABC_International_List_of_Valid_Browsers.txt", null));
    filterRules.get(ChannelType.EPN).put(IABRobotRule.class.getSimpleName(), new FilterRuleContent
        (IABRobotRule.class.getSimpleName(), "IAB_ABC_International_Spiders_and_Robots.txt"));
  }

  /**
   * Two-pass Test
   */
  @Test
  public void testTwoPassEmpty() {
    BaseFilterRule wrule = TwoPassIABRule.createForTest(ChannelType.EPN);
    FilterRule rule = wrule;
    FilterRequest req = new FilterRequest();
    assertEquals(1, rule.test(req));
  }

  @Test
  public void testWhitelist() {
    TwoPassIABRule rule = TwoPassIABRule.createForTest(ChannelType.EPN);
    FilterRequest req1 = new FilterRequest();
    req1.setUserAgent("foo");
    FilterRequest req2 = new FilterRequest();
    req2.setUserAgent("bar");
    assertEquals(1, rule.test(req1));
    assertEquals(1, rule.test(req2));
    rule.addWhitelistEntry("bar|1|0");
    assertEquals(1, rule.test(req1));
    assertEquals(0, rule.test(req2));
    rule.clear();
    assertEquals(1, rule.test(req1));
    assertEquals(1, rule.test(req1));
  }

  @Test
  public void testBlacklist() {
    TwoPassIABRule rule = TwoPassIABRule.createForTest(ChannelType.EPN);
    FilterRequest req1 = new FilterRequest();
    req1.setUserAgent("foo");
    FilterRequest req2 = new FilterRequest();
    req2.setUserAgent("bar");
    rule.addWhitelistEntry("foo|1|0");
    rule.addWhitelistEntry("bar|1|0");
    rule.addBlacklistEntry("bar|1||0|2|0");
    assertEquals(0, rule.test(req1));
    assertEquals(1, rule.test(req2));
    rule.clear();
    rule.addWhitelistEntry("foo|1|0");
    rule.addWhitelistEntry("bar|1|0");
    assertEquals(0, rule.test(req1));
    assertEquals(0, rule.test(req2));
  }

  @Test
  public void testIABReadFromFiles() {
    TwoPassIABRule rule = TwoPassIABRule.createForTest(ChannelType.EPN);
    String blStr = "bar|1||0|2|0\n#zyx|1||0|2|0";
    String wlStr = "foo|1|0\n#qizzy|1|0";
    rule.readFromStrings(wlStr, blStr);
    FilterRequest req = new FilterRequest();
    // whitelist
    req.setUserAgent("foo");
    assertEquals(0, rule.test(req));
    // #whitelist
    req.setUserAgent("qizzy");
    assertEquals(1, rule.test(req));
    // whitelist, blacklist
    req.setUserAgent("foobar");
    assertEquals(1, rule.test(req));
    // whitelist, #blacklist
    req.setUserAgent("foozyx");
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testIABReadFromFiles2() {
    TwoPassIABRule rule = TwoPassIABRule.createForTest(ChannelType.EPN);
    String blStr = "obot|1|robotics, TheRobotFree|0|0|0\npita|1|spital, Capital|0|1|0\nsohu|1|SohuEnNews, SOHUVideo, SohuNews|0|0|0\nspider|1|SpiderSolitaire, GLX Spider, GLX+Spider|0|2|0\nstuff|1|StuffNZ|0|2|0";
    String wlStr = "iCab|1|0\niLiga|1|0\niPhone|1|0\niPod touch|1|0\niPod+touch|1|0\niTunes|1|0";
    rule.readFromStrings(wlStr, blStr);
    FilterRequest req = new FilterRequest();
    req.setUserAgent("iPhone SohuEnNews");
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testValidBrowserRule() {
    ValidBrowserRule rule = ValidBrowserRule.createForTest(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    assertEquals(1, rule.test(req));
    req.setUserAgent("bar");
    assertEquals(1, rule.test(req));
    rule.addWhitelistEntry("bar|1|1");
    assertEquals(0, rule.test(req));
    req.setUserAgent("AU-MIC");
    assertEquals(0, rule.test(req));
    rule.clear();
    assertEquals(1, rule.test(req));
  }

  @Test
  public void testIABRobotRule() {
    IABRobotRule rule = IABRobotRule.createForTest(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    assertEquals(1, rule.test(req));
    req.setUserAgent("bar");
    assertEquals(0, rule.test(req));
    rule.addBlacklistEntry("bar|1||0|2|0");
    assertEquals(1, rule.test(req));
    req.setUserAgent("internetseer");
    assertEquals(1, rule.test(req));
    rule.clear();
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testEBayRobotRule() {
    EBayRobotRule rule = EBayRobotRule.createForTest(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    assertEquals(1, rule.test(req));
    req.setUserAgent("bar");
    assertEquals(0, rule.test(req));
    rule.addBlacklistEntry("bar|1|1");
    assertEquals(1, rule.test(req));
    rule.clear();
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testBlacklistReadFromString() {
    GenericBlacklistRule rule = new TestBlacklistRule(ChannelType.EPN);
    rule.readFromString("foo\nBAR\r#baz\nzyx");
    assertEquals(true, rule.contains("foo"));
    assertEquals(true, rule.contains("bar"));
    assertEquals(true, rule.contains("zyx"));
    assertEquals(false, rule.contains("baz"));
  }

  @Test
  public void testIPBlacklistRule() {
    IPBlacklistRule rule = new IPBlacklistRule(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    req.setSourceIP("foo");
    assertEquals(0, rule.test(req));
    rule.add("foo");
    assertEquals(1, rule.test(req));
    rule.clear();
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testEPNDomainBlacklistRule() {
    EPNDomainBlacklistRule rule = EPNDomainBlacklistRule.createForTest(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    req.setReferrerDomain("foo");
    assertEquals(0, rule.test(req));
    rule.add("FOO");
    assertEquals(1, rule.test(req));
    rule.clear();
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testEBayRefererDomainRule() {
    EBayRefererDomainRule rule = new EBayRefererDomainRule(ChannelType.EPN);
    ListenerMessage msg = new ListenerMessage();
    msg.setReferer("www.bbc.co.ukebay");
    FilterRequest req = new FilterRequest(msg);
    assertEquals(1, rule.test(req));
    req.setReferrerDomain(null);
    assertEquals(0, rule.test(req));
    req.setReferrerDomain("foo");
    assertEquals(0, rule.test(req));
    rule.add("FOO");
    assertEquals(1, rule.test(req));
    rule.clear();
    assertEquals(0, rule.test(req));
    req.setReferrerDomain("m.ebay.com");
    assertEquals(1,rule.test(req));
  }

  private class TestBlacklistRule extends GenericBlacklistRule {
    public TestBlacklistRule(ChannelType channelType) {
      super(channelType);
    }

    @Override
    public boolean isChannelActionApplicable(ChannelAction action) { return true; }

    @Override
    protected String getFilteredValue(FilterRequest request) { return null; }
  }
}