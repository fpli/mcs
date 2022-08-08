package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by spugach on 5/3/17.
 */
public class WeightedRuleTest {

  @BeforeClass
  public static void setUp() throws IOException {
    FilterRuleMgr.getInstance().initFilterRuleConfig("filter_rule_config.json");
  }

  @Before
  public void addTestRules() throws IOException {
    //Default Testing Data from configuration file
    Map<ChannelType, Map<String, FilterRuleContent>> filterRules = FilterRuleMgr.getInstance().getFilterRuleConfigMap();

    //Testing Data for EPN channel
    filterRules.get(ChannelType.EPN).put(TestRule1.class.getSimpleName(), new FilterRuleContent
        (TestRule1.class.getSimpleName()));
  }

  @Test
  public void rulesIgnoreAppDL() {
    FilterRule rule = new TestRule1(ChannelType.EPN);

    assertTrue(rule.isChannelActionApplicable(ChannelAction.CLICK));
    assertTrue(rule.isChannelActionApplicable(ChannelAction.IMPRESSION));
    assertTrue(rule.isChannelActionApplicable(ChannelAction.SERVE));
    assertFalse(rule.isChannelActionApplicable(ChannelAction.APP_FIRST_START));
  }

  public class TestRule extends BaseFilterRule {
    public TestRule(ChannelType channelType) {
      super(channelType);
    }

    @Override
    public int test(FilterRequest event) { return 0; }
  }

  public class TestRule1 extends TestRule {
    public TestRule1(ChannelType channelType) {
      super(channelType);
    }
  }

  public class TestRule2 extends TestRule {
    public TestRule2(ChannelType channelType) {
      super(channelType);
    }
  }
}
