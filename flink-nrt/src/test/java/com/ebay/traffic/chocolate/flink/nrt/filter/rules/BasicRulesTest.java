package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
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
 * Created by spugach on 11/29/16.
 */
public class BasicRulesTest {

  @BeforeClass
  public static void setUp() throws IOException {
    FilterRuleMgr.getInstance().initFilterRuleConfig("filter_rule_config.json");
  }

  @Before
  public void addTestRules() {
    Map<ChannelType, Map<String, FilterRuleContent>> filterRules = FilterRuleMgr.getInstance().getFilterRuleConfigMap();
    filterRules.get(ChannelType.EPN).put(CguidStalenessWindowRule.class.getSimpleName(), new FilterRuleContent
        (CguidStalenessWindowRule.class.getSimpleName(), null, null, 500l));
    filterRules.get(ChannelType.EPN).put(CampaignClickThroughRateRule.class.getSimpleName(), new FilterRuleContent
        (CampaignClickThroughRateRule.class.getSimpleName(), null, 0.01f, null));
    filterRules.get(ChannelType.EPN).put(ProtocolRule.class.getSimpleName(), new FilterRuleContent
        (ProtocolRule.class.getSimpleName()));
    filterRules.get(ChannelType.EPN).put(MissingRefererRule.class.getSimpleName(), new FilterRuleContent
        (MissingRefererRule.class.getSimpleName()));
  }

  @Test
  public void testPrefetchRule() {
    BaseFilterRule wrule = new PrefetchRule(ChannelType.EPN);
    FilterRule rule = wrule;

    FilterRequest req = new FilterRequest();
    assertEquals(0, rule.test(req));
    req.setPrefetch(true);
    assertEquals(1, rule.test(req));
  }

  @Test
  public void testProtocolRule() {
    BaseFilterRule wrule = new ProtocolRule(ChannelType.EPN);
    FilterRule rule = wrule;
    FilterRequest req = new FilterRequest();
    assertEquals(1, rule.test(req));
    req.setProtocol(HttpMethod.GET);
    assertEquals(0, rule.test(req));
    req.setProtocol(HttpMethod.POST);
    assertEquals(0, rule.test(req));
    req.setProtocol(HttpMethod.HEAD);
    assertEquals(1, rule.test(req));
    req.setProtocol(HttpMethod.PUT);
    assertEquals(1, rule.test(req));
  }

  @Test
  public void testMissingRefererRule() {
    MissingRefererRule rule = new MissingRefererRule(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    assertEquals(1, rule.test(req));
    req.setReferrerDomain("foo");
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testCGUIDTimestampWindow() {
    BaseFilterRule wrule = new CguidStalenessWindowRule(ChannelType.EPN);
    FilterRule rule = wrule;
    FilterRequest req = new FilterRequest();
    req.setChannelAction(ChannelAction.CLICK);
    req.setRequestCguid("foo");
    req.setRequestCguidTimestamp(10000L);
    req.setTimestamp(10000L + 490);                // just before the window start
    assertEquals(1, rule.test(req));
    req.setTimestamp(10000L + 510);                // just after the window start
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testPublisherValidRule() {
    FilterRequest req = new FilterRequest();
    BaseFilterRule wrule = new PublisherValidRule(ChannelType.EPN);
    FilterRule rule = wrule;

    req.setPublisherId(-100l);
    assertEquals(1, rule.test(req));
    req.setPublisherId(-1l);
    assertEquals(1, rule.test(req));
    req.setPublisherId(0);
    assertEquals(0, rule.test(req));
    req.setPublisherId(1);
    assertEquals(0, rule.test(req));
  }

  @Test
  public void testCampaignClickThroughRuleCutoff() {
    BaseFilterRule wrule = new CampaignClickThroughRateRule(ChannelType.EPN);
    FilterRule rule = wrule;

    // Test that as soon as the ratio is exceeded, the rule fails
    FilterRequest req = new FilterRequest();
    req.setCampaignId(1);
    req.setChannelAction(ChannelAction.IMPRESSION);
    for (int i = 0; i < 198; ++i) {
      assertEquals(0, rule.test(req));
    }

    req.setChannelAction(ChannelAction.CLICK);
    assertEquals(0, rule.test(req));
    assertEquals(1, rule.test(req));
    assertEquals(1, rule.test(req));
  }

  @Test
  public void testCampaignClickThroughRuleMinimumToCheck() {
    FilterRule rule = new CampaignClickThroughRateRule(ChannelType.EPN);

    FilterRequest req = new FilterRequest();
    req.setChannelAction(ChannelAction.CLICK);
    req.setCampaignId(2);

    // Test the first 99 events do not fail no matter what the ratio is
    for (int i = 0; i < 50; ++i) {
      assertEquals(0, rule.test(req));
    }
    req.setChannelAction(ChannelAction.IMPRESSION);
    for (int i = 0; i < 49; ++i) {
      assertEquals(0, rule.test(req));
    }

    // The 100th fails
    assertEquals(1, rule.test(req));
  }

  @Test
  public void testCampaignClickThroughRuleParallel() {
    FilterRule rule1 = new CampaignClickThroughRateRule(ChannelType.EPN);
    FilterRule rule2 = new CampaignClickThroughRateRule(ChannelType.EPN);

    // Test that the two rule instances share the log
    FilterRequest req = new FilterRequest();
    req.setCampaignId(3);
    req.setChannelAction(ChannelAction.CLICK);
    assertEquals(0, rule2.test(req));
    for (int i = 0; i < 100; ++i) {
      rule1.test(req);
    }

    assertEquals(1, rule2.test(req));
  }

  @Test
  public void testCampaignClickThroughRuleCampaignDependent() {
    FilterRule rule = new CampaignClickThroughRateRule(ChannelType.EPN);

    // Test that logging is campaign dependent
    FilterRequest req = new FilterRequest();
    req.setCampaignId(4);
    req.setChannelAction(ChannelAction.CLICK);
    for (int i = 0; i < 100; ++i) {
      rule.test(req);
    }

    req.setCampaignId(5);
    assertEquals(0, rule.test(req));
  }

  @Test
  public void internalTrafficRulePassesExternal() {
    FilterRule rule = new InternalTrafficRule(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    req.setSourceIP("204.79.197.200");
    req.setReferrerDomain("www.bing.com");
    assertEquals(0, rule.test(req));
    req.setSourceIP("");
    req.setReferrerDomain("www.bing.com");
    assertEquals(0, rule.test(req));
  }

  @Test
  public void internalTrafficRuleFailsInternal() {
    FilterRule rule = new InternalTrafficRule(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    req.setSourceIP("192.168.0.1");
    req.setReferrerDomain("www.bing.com");
    assertEquals(1, rule.test(req));
    req.setSourceIP("127.0.0.1");
    assertEquals(1, rule.test(req));
    req.setSourceIP("10.64.251.5");       // In-VPN IP
    assertEquals(1, rule.test(req));
    req.setSourceIP("204.79.197.200");
    req.setReferrerDomain("chocolate-qa-slc-1-4595.slc01.dev.ebayc3.com");
    assertEquals(0, rule.test(req));
    req.setReferrerDomain("localhost");
    assertEquals(1, rule.test(req));
  }
}