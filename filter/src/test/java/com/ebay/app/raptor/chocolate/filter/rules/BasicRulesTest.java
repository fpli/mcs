package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterWeightedRule;
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
 * Created by spugach on 11/29/16.
 */
public class BasicRulesTest {

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(BasicRulesTest.class.getClassLoader().getResource("META-INF/configuration/Dev/"));
    ApplicationOptions.initFilterRuleConfig("filter_rule_config.json");
  }

  @Before
  public void addTestRules() throws IOException {
    Map<ChannelType, Map<String, FilterRuleContent>> filterRules = ApplicationOptions.filterRuleConfigMap;
    filterRules.get(ChannelType.EPN).put(CGUIDStalenessWindowRule.class.getSimpleName(), new FilterRuleContent
        (CGUIDStalenessWindowRule.class.getSimpleName(), 1.0f, null, 500l, null));
    filterRules.get(ChannelType.EPN).put(CampaignClickThroughRateRule.class.getSimpleName(), new FilterRuleContent
        (CampaignClickThroughRateRule.class.getSimpleName(), 1.3f, 0.01f, null, null));
    if (filterRules.get(ChannelType.DISPLAY) == null) {
      filterRules.put(ChannelType.DISPLAY, new HashMap<String, FilterRuleContent>());
    }
    filterRules.get(ChannelType.DISPLAY).put(CGUIDStalenessWindowRule.class.getSimpleName(), new FilterRuleContent
        (CGUIDStalenessWindowRule.class.getSimpleName(), 1.5f));
    filterRules.get(ChannelType.DISPLAY).put(InternalTrafficRule.class.getSimpleName(), new FilterRuleContent
        (InternalTrafficRule.class.getSimpleName(), 2.0f));
  }
  
  @Test
  public void testPrefetchRule() {
    BaseFilterWeightedRule wrule = new PrefetchRule(ChannelType.EPN);
    FilterRule rule = wrule;
    
    FilterRequest req = new FilterRequest();
    assertEquals(0, rule.test(req), 0.001);
    req.setPrefetch(true);
    assertEquals(1, rule.test(req), 0.001);
    
    // DISPLAY Channel
    wrule = new PrefetchRule(ChannelType.DISPLAY);
    rule = wrule;
    req = new FilterRequest();
    assertEquals(0, rule.test(req), 0.001);
    req.setPrefetch(true);
    assertEquals(1, rule.test(req), 0.001);
  }
  
  @Test
  public void testCGUIDTimestampWindow() {
    BaseFilterWeightedRule wrule = new CGUIDStalenessWindowRule(ChannelType.EPN);
    FilterRule rule = wrule;
    FilterRequest req = new FilterRequest();
    req.setChannelAction(ChannelAction.CLICK);
    req.setRequestCGUID("foo");
    req.setRequestCGUIDTimestamp(10000L);
    req.setTimestamp(10000L + 490);                // just before the window start
    assertEquals(1, rule.test(req), 0.001);
    req.setTimestamp(10000L + 510);                // just after the window start
    assertEquals(0, rule.test(req), 0.001);
    
    // DISPLAY Channel
    wrule = new CGUIDStalenessWindowRule(ChannelType.DISPLAY);
    rule = wrule;
    req = new FilterRequest();
    req.setChannelAction(ChannelAction.CLICK);
    req.setRequestCGUID("foo");
    req.setRequestCGUIDTimestamp(10000L);
    
    req.setTimestamp(10000L + 490);                // just before the window start
    assertEquals(1.5, rule.test(req), 0.001);
    req.setTimestamp(10000L + 510);                // just after the window start
    assertEquals(0, rule.test(req), 0.001);
  }
  
  @Test
  public void testPublisherValidRule() {
    FilterRequest req = new FilterRequest();
    BaseFilterWeightedRule wrule = new PublisherValidRule(ChannelType.EPN);
    FilterRule rule = wrule;
    
    req.setPublisherId(-100l);
    assertEquals(1.0, rule.test(req), 0.001);
    req.setPublisherId(-1l);
    assertEquals(1.0, rule.test(req), 0.001);
    req.setPublisherId(0);
    assertEquals(0, rule.test(req), 0.001);
    req.setPublisherId(1);
    assertEquals(0, rule.test(req), 0.001);
    
    // DISPLAY Channel
    wrule = new PublisherValidRule(ChannelType.DISPLAY);
    rule = wrule;
    req.setPublisherId(-100l);
    assertEquals(0, rule.test(req), 0.001);
    req.setPublisherId(-1l);
    assertEquals(0, rule.test(req), 0.001);
    req.setPublisherId(0);
    assertEquals(0, rule.test(req), 0.001);
    req.setPublisherId(1);
    assertEquals(0, rule.test(req), 0.001);
  }
  
  @Test
  public void testCampaignClickThroughRuleCutoff() {
    BaseFilterWeightedRule wrule = new CampaignClickThroughRateRule(ChannelType.EPN);
    FilterRule rule = wrule;
    
    // Test that as soon as the ratio is exceeded, the rule fails
    FilterRequest req = new FilterRequest();
    req.setCampaignId(1);
    req.setChannelAction(ChannelAction.IMPRESSION);
    for (int i = 0; i < 198; ++i) {
      assertEquals(0, rule.test(req), 0.001);
    }
    
    req.setChannelAction(ChannelAction.CLICK);
    assertEquals(0, rule.test(req), 0.001);
    assertEquals(1.3, rule.test(req), 0.001);
    assertEquals(1.3, rule.test(req), 0.001);
    
    // DISPLAY Channel
    wrule = new CampaignClickThroughRateRule(ChannelType.DISPLAY);
    rule = wrule;
    req.setCampaignId(1);
    req.setChannelAction(ChannelAction.IMPRESSION);
    for (int i = 0; i < 198; ++i) {
      assertEquals(0, rule.test(req), 0.001);
    }
    
    req.setChannelAction(ChannelAction.CLICK);
    assertEquals(0, rule.test(req), 0.001);
    assertEquals(0, rule.test(req), 0.001);
    assertEquals(0, rule.test(req), 0.001);
  }
  
  @Test
  public void testCampaignClickThroughRuleMinimumToCheck() {
    FilterRule rule = new CampaignClickThroughRateRule(ChannelType.EPN);
    
    FilterRequest req = new FilterRequest();
    req.setChannelAction(ChannelAction.CLICK);
    req.setCampaignId(2);
    
    // Test the first 99 events do not fail no matter what the ratio is
    for (int i = 0; i < 50; ++i) {
      assertEquals(0, rule.test(req), 0.001);
    }
    req.setChannelAction(ChannelAction.IMPRESSION);
    for (int i = 0; i < 49; ++i) {
      assertEquals(0, rule.test(req), 0.001);
    }
    
    // The 100th fails
    assertEquals(1.3, rule.test(req), 0.001);
    
    // DISPLAY Channel
    rule = new CampaignClickThroughRateRule(ChannelType.DISPLAY);
    req = new FilterRequest();
    req.setChannelAction(ChannelAction.CLICK);
    req.setCampaignId(2);
    // Test the first 99 events do not fail no matter what the ratio is
    for (int i = 0; i < 50; ++i) {
      assertEquals(0, rule.test(req), 0.001);
    }
    req.setChannelAction(ChannelAction.IMPRESSION);
    for (int i = 0; i < 49; ++i) {
      assertEquals(0, rule.test(req), 0.001);
    }
    
    // The 100th fails
    assertEquals(0, rule.test(req), 0.001);
  }
  
  @Test
  public void testCampaignClickThroughRuleParallel() {
    FilterRule rule1 = new CampaignClickThroughRateRule(ChannelType.EPN);
    FilterRule rule2 = new CampaignClickThroughRateRule(ChannelType.EPN);
    
    // Test that the two rule instances share the log
    FilterRequest req = new FilterRequest();
    req.setCampaignId(3);
    req.setChannelAction(ChannelAction.CLICK);
    assertEquals(0, rule2.test(req), 0.001);
    for (int i = 0; i < 100; ++i) {
      rule1.test(req);
    }
    
    assertEquals(1.3, rule2.test(req), 0.001);
    
    //DISPLAY Channel
    rule1 = new CampaignClickThroughRateRule(ChannelType.DISPLAY);
    rule2 = new CampaignClickThroughRateRule(ChannelType.DISPLAY);
    
    // Test that the two rule instances share the log
    req = new FilterRequest();
    req.setCampaignId(3);
    req.setChannelAction(ChannelAction.CLICK);
    assertEquals(0, rule2.test(req), 0.001);
    for (int i = 0; i < 100; ++i) {
      rule1.test(req);
    }
    
    assertEquals(0, rule2.test(req), 0.001);
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
    assertEquals(0, rule.test(req), 0.001);
  }
  
  @Test
  public void internalTrafficRulePassesExternal() {
    FilterRule rule = new InternalTrafficRule(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    req.setSourceIP("204.79.197.200");
    req.setReferrerDomain("www.bing.com");
    assertEquals(0, rule.test(req), 0.001);
    
    rule = new InternalTrafficRule(ChannelType.DISPLAY);
    req = new FilterRequest();
    req.setSourceIP("204.79.197.200");
    req.setReferrerDomain("www.bing.com");
    assertEquals(0, rule.test(req), 0.001);
  }
  
  @Test
  public void internalTrafficRuleFailsInternal() {
    FilterRule rule = new InternalTrafficRule(ChannelType.EPN);
    FilterRequest req = new FilterRequest();
    req.setSourceIP("192.168.0.1");
    req.setReferrerDomain("www.bing.com");
    assertEquals(1.0f, rule.test(req), 0.001);
    req.setSourceIP("127.0.0.1");
    assertEquals(1.0f, rule.test(req), 0.001);
    req.setSourceIP("10.64.251.5");       // In-VPN IP
    assertEquals(1.0f, rule.test(req), 0.001);
    req.setSourceIP("204.79.197.200");
    req.setReferrerDomain("chocolate-qa-slc-1-4595.slc01.dev.ebayc3.com");
    assertEquals(0.0f, rule.test(req), 0.001);
    req.setReferrerDomain("localhost");
    assertEquals(1.0f, rule.test(req), 0.001);
    
    rule = new InternalTrafficRule(ChannelType.DISPLAY);
    req = new FilterRequest();
    req.setSourceIP("192.168.0.1");
    req.setReferrerDomain("www.bing.com");
    assertEquals(2.0f, rule.test(req), 0.001);
    req.setSourceIP("127.0.0.1");
    assertEquals(2.0f, rule.test(req), 0.001);
    req.setSourceIP("10.64.251.5");       // In-VPN IP
    assertEquals(2.0f, rule.test(req), 0.001);
    req.setSourceIP("204.79.197.200");
    req.setReferrerDomain("chocolate-qa-slc-1-4595.slc01.dev.ebayc3.com");
    assertEquals(0.0f, rule.test(req), 0.001);
    req.setReferrerDomain("localhost");
    assertEquals(2.0f, rule.test(req), 0.001);
  }
}
