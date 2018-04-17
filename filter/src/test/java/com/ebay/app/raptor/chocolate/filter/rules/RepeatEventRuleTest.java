package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.app.raptor.chocolate.filter.service.FilterRule;
import com.ebay.kernel.context.RuntimeContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by spugach on 12/19/16.
 */
public class RepeatEventRuleTest {
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
        FilterRuleContent ruleConentEPN = new FilterRuleContent();
        ruleConentEPN.setRuleName(RepeatClickRule.class.getSimpleName());
        ruleConentEPN.setTimeoutMS(1000);
        filterRules.get(ChannelType.EPN).put(RepeatClickRule.class.getSimpleName(), ruleConentEPN);

        //Testing Data for DAP channel
        FilterRuleContent ruleConentDAP = new FilterRuleContent();
        ruleConentDAP.setTimeoutMS(1500);
        filterRules.get(ChannelType.DISPLAY).put(RepeatClickRule.class.getSimpleName(), ruleConentDAP);

    }

    @After
    public void cleanup() {
        RepeatClickRule.clear();
    }

    @Test
    public void testRepeatClickTriggerAndExpiry(){
        FilterRule ruleEPN= new RepeatClickRule(ChannelType.EPN);
        FilterRequest req = new FilterRequest();
        req.setResponseCGUID("epn1");
        req.setChannelAction(ChannelAction.CLICK);
        FilterRequest req2 = new FilterRequest();
        req2.setResponseCGUID("epn2");
        req2.setChannelAction(ChannelAction.CLICK);
        req.setTimestamp(1000);
        assertEquals(0, ruleEPN.test(req), 0.001);
        req2.setTimestamp(1900);
        assertEquals(0, ruleEPN.test(req2), 0.001);
        req.setTimestamp(1900);
        assertEquals(1, ruleEPN.test(req), 0.001);
        req2.setTimestamp(3000);
        assertEquals(0, ruleEPN.test(req2), 0.001);
        req.setTimestamp(3000);
        assertEquals(0, ruleEPN.test(req), 0.001);
    }

    @Test
    public void testSharedMemory(){
        FilterRule rule1 = new RepeatClickRule(ChannelType.EPN);
        FilterRule rule2 = new RepeatClickRule(ChannelType.EPN);

        FilterRequest req = new FilterRequest();
        req.setResponseCGUID("aaa");
        req.setChannelAction(ChannelAction.CLICK);
        FilterRequest req2 = new FilterRequest();
        req2.setResponseCGUID("bbb");
        req2.setChannelAction(ChannelAction.CLICK);
        req.setTimestamp(1000);
        req2.setTimestamp(1000);
        assertEquals(0, rule1.test(req), 0.001);
        assertEquals(0, rule2.test(req2), 0.001);
        req.setTimestamp(1900);
        req2.setTimestamp(1900);
        assertEquals(1, rule2.test(req), 0.001);
        assertEquals(1, rule1.test(req), 0.001);
    }
}
