package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.app.raptor.chocolate.filter.service.FilterRule;
import com.ebay.kernel.context.RuntimeContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by spugach don 11/30/16.
 */
public class BlacklistRulesTest {

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(BasicRulesTest.class.getClassLoader().getResource("META-INF/configuration/Dev/"));
    ApplicationOptions.initFilterRuleConfig("filter_rule_config.json");
  }

  @Before
  public void addTestRules() throws IOException {
    //Default Testing Data
    Map<ChannelType, Map<String, FilterRuleContent>> filterRules = ApplicationOptions.filterRuleConfigMap;

    //Testing Data for EPN channel
    filterRules.get(ChannelType.EPN).put(TwoPassIABRule.class.getSimpleName(), new FilterRuleContent
        (TwoPassIABRule.class.getSimpleName()));
    filterRules.get(ChannelType.EPN).put(TestBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (TestBlacklistRule.class.getSimpleName()));
    filterRules.get(ChannelType.EPN).put(IPBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (IPBlacklistRule.class.getSimpleName()));
    filterRules.get(ChannelType.EPN).put(EPNDomainBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (EPNDomainBlacklistRule.class.getSimpleName()));

    //Testing Data for DAP channel
    if (filterRules.get(ChannelType.DISPLAY) == null) {
      filterRules.put(ChannelType.DISPLAY, new HashMap<String, FilterRuleContent>());
    }
    filterRules.get(ChannelType.DISPLAY).put(TwoPassIABRule.class.getSimpleName(), new FilterRuleContent
        (TwoPassIABRule.class.getSimpleName()));
    filterRules.get(ChannelType.DISPLAY).put(TestBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (TestBlacklistRule.class.getSimpleName()));
    filterRules.get(ChannelType.DISPLAY).put(IPBlacklistRule.class.getSimpleName(), new FilterRuleContent
        (IPBlacklistRule.class.getSimpleName()));
  }

  /**
   * Two-pass Test
   */
  @Test
  public void testTwoPassEmpty() {
    BaseFilterRule wrule = TwoPassIABRule.createForTest(ChannelType.EPN);
    FilterRule rule = wrule;
    FilterRequest req = new FilterRequest();
    assertEquals(1, rule.test(req), 0.001);

    wrule = TwoPassIABRule.createForTest(ChannelType.DISPLAY);
    rule = wrule;
    assertEquals(1, rule.test(req), 0.001);
  }

  @Test
  public void testWhitelist() {
    TwoPassIABRule rule = TwoPassIABRule.createForTest(ChannelType.EPN);
    FilterRequest req1 = new FilterRequest();
    req1.setUserAgent("foo");
    FilterRequest req2 = new FilterRequest();
    req2.setUserAgent("bar");
    assertEquals(1, rule.test(req1), 0.001);
    assertEquals(1, rule.test(req2), 0.001);
    rule.addWhitelistEntry("bar|1|0");
    assertEquals(1, rule.test(req1), 0.001);
    assertEquals(0, rule.test(req2), 0.001);
    rule.clear();
    assertEquals(1, rule.test(req1), 0.001);
    assertEquals(1, rule.test(req1), 0.001);

    rule = TwoPassIABRule.createForTest(ChannelType.DISPLAY);
    assertEquals(1, rule.test(req1), 0.001);
    assertEquals(1, rule.test(req2), 0.001);
    rule.addWhitelistEntry("bar|1|0");
    assertEquals(1, rule.test(req1), 0.001);
    assertEquals(0, rule.test(req2), 0.001);
    rule.clear();
    assertEquals(1, rule.test(req1), 0.001);
    assertEquals(1, rule.test(req1), 0.001);
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
    assertEquals(0, rule.test(req1), 0.001);
    assertEquals(1, rule.test(req2), 0.001);
    rule.clear();
    rule.addWhitelistEntry("foo|1|0");
    rule.addWhitelistEntry("bar|1|0");
    assertEquals(0, rule.test(req1), 0.001);
    assertEquals(0, rule.test(req2), 0.001);

    rule.clear();
    rule = TwoPassIABRule.createForTest(ChannelType.DISPLAY);
    rule.addWhitelistEntry("foo|1|0");
    rule.addWhitelistEntry("bar|1|0");
    rule.addBlacklistEntry("bar|1||0|2|0");
    assertEquals(0, rule.test(req1), 0.001);
    assertEquals(1, rule.test(req2), 0.001);
    rule.clear();
    rule.addWhitelistEntry("foo|1|0");
    rule.addWhitelistEntry("bar|1|0");
    assertEquals(0, rule.test(req1), 0.001);
    assertEquals(0, rule.test(req2), 0.001);
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
    assertEquals(0, rule.test(req), 0.001);
    // #whitelist
    req.setUserAgent("qizzy");
    assertEquals(1, rule.test(req), 0.001);
    // whitelist, blacklist
    req.setUserAgent("foobar");
    assertEquals(1, rule.test(req), 0.001);
    // whitelist, #blacklist
    req.setUserAgent("foozyx");
    assertEquals(0, rule.test(req), 0.001);

    rule = TwoPassIABRule.createForTest(ChannelType.DISPLAY);
    rule.readFromStrings(wlStr, blStr);
    // whitelist
    req.setUserAgent("foo");
    assertEquals(0, rule.test(req), 0.001);
    // #whitelist
    req.setUserAgent("qizzy");
    assertEquals(1, rule.test(req), 0.001);
    // whitelist, blacklist
    req.setUserAgent("foobar");
    assertEquals(1, rule.test(req), 0.001);
    // whitelist, #blacklist
    req.setUserAgent("foozyx");
    assertEquals(0, rule.test(req), 0.001);
  }

  @Test
  public void testIABReadFromFiles2() {
    TwoPassIABRule rule = TwoPassIABRule.createForTest(ChannelType.EPN);
    String blStr = "obot|1|robotics, TheRobotFree|0|0|0\npita|1|spital, Capital|0|1|0\nsohu|1|SohuEnNews, SOHUVideo, SohuNews|0|0|0\nspider|1|SpiderSolitaire, GLX Spider, GLX+Spider|0|2|0\nstuff|1|StuffNZ|0|2|0";
    String wlStr = "iCab|1|0\niLiga|1|0\niPhone|1|0\niPod touch|1|0\niPod+touch|1|0\niTunes|1|0";
    rule.readFromStrings(wlStr, blStr);
    FilterRequest req = new FilterRequest();
    req.setUserAgent("iPhone SohuEnNews");
    assertEquals(0, rule.test(req), 0.001);

    rule = TwoPassIABRule.createForTest(ChannelType.DISPLAY);
    rule.readFromStrings(wlStr, blStr);
    req.setUserAgent("iPhone SohuEnNews");
    assertEquals(0, rule.test(req), 0.001);
  }

  @Test
  public void testBlacklistReadFromString() {
    GenericBlacklistRule rule = new TestBlacklistRule(ChannelType.EPN);
    FilterRule irule = rule;
    rule.readFromString("foo\nBAR\r#baz\nzyx");
    assertEquals(true, rule.contains("foo"));
    assertEquals(true, rule.contains("bar"));
    assertEquals(true, rule.contains("zyx"));
    assertEquals(false, rule.contains("baz"));

    rule = new TestBlacklistRule(ChannelType.DISPLAY);
    irule = rule;
    rule.readFromString("foo\nBAR\r#baz\nzyx");
    assertEquals(true, rule.contains("foo"));
    assertEquals(true, rule.contains("bar"));
    assertEquals(true, rule.contains("zyx"));
    assertEquals(false, rule.contains("baz"));
  }

  ;

  @Test
  public void testIPBlacklistRule() {
    IPBlacklistRule rule = new IPBlacklistRule(ChannelType.EPN);
    FilterRule irule = rule;
    FilterRequest req = new FilterRequest();
    req.setSourceIP("foo");
    assertEquals(0, rule.test(req), 0.001);
    rule.add("foo");
    assertEquals(1, rule.test(req), 0.001);
    rule.clear();
    assertEquals(0, rule.test(req), 0.001);


    rule = new IPBlacklistRule(ChannelType.DISPLAY);
    irule = rule;
    req.setSourceIP("foo");
    assertEquals(0, rule.test(req), 0.001);
    rule.add("foo");
    assertEquals(1, rule.test(req), 0.001);
    rule.clear();
    assertEquals(0, rule.test(req), 0.001);
  }

  @Test
  public void testEbayDomainBlacklistRule() {
    EPNDomainBlacklistRule rule = EPNDomainBlacklistRule.createForTest(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    req.setReferrerDomain("foo");
    assertEquals(0, rule.test(req), 0.001);
    rule.add("FOO");
    assertEquals(1, rule.test(req), 0.001);
    rule.clear();
    assertEquals(0, rule.test(req), 0.001);


    rule = EPNDomainBlacklistRule.createForTest(ChannelType.DISPLAY);
    rule.clear();
    req.setReferrerDomain("foo");
    assertEquals(0, rule.test(req), 0.001);
    rule.add("FOO");
    assertEquals(1, rule.test(req), 0.001);
    rule.clear();
    assertEquals(0, rule.test(req), 0.001);
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
