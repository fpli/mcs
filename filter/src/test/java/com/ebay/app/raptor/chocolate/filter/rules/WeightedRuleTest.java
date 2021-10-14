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

import static org.junit.Assert.*;

/**
 * Created by spugach on 5/3/17.
 */
public class WeightedRuleTest {

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(BasicRulesTest.class.getClassLoader().getResource("META-INF/configuration/Dev/"));
    ApplicationOptions.initFilterRuleConfig("filter_rule_config.json");
  }

  @Before
  public void addTestRules() throws IOException {
    //Default Testing Data from configuration file
    Map<ChannelType, Map<String, FilterRuleContent>> filterRules = ApplicationOptions.filterRuleConfigMap;

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
