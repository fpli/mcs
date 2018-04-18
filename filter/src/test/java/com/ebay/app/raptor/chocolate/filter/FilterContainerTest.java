package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;
import com.ebay.app.raptor.chocolate.filter.service.*;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

/**
 * Created by spugach on 11/22/16.
 */
public class FilterContainerTest {

  @Test
  public void testBasicRuleList() {
    FilterContainer filter = new FilterContainer();
    assertThat(filter, instanceOf(HashMap.class));
    ListenerMessage lm = new ListenerMessage();
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    lm.setChannelType(ChannelType.DEFAULT);
    HashMap<FilterRuleType, FilterRule> ruleMap = new HashMap<FilterRuleType, FilterRule>();
    ruleMap.put(FilterRuleType.PREFETCH, new BaseRuleMock(1));
    filter.put(ChannelType.DEFAULT, ruleMap);

    long result= filter.test(lm);
    assertEquals(2, result);
  }
  
  @Test
  public void testRuleListPassWithRule() throws IOException {
    FilterContainer filter = new FilterContainer();
    ListenerMessage lm = new ListenerMessage();
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    lm.setChannelType(ChannelType.DEFAULT);
    HashMap<FilterRuleType, FilterRule> ruleMap = new HashMap<FilterRuleType, FilterRule>();
    ruleMap.put(FilterRuleType.PREFETCH, new BaseRuleMock(0));
    filter.put(ChannelType.DEFAULT, ruleMap);
    
    long result= filter.test(lm);
    assertEquals(0, result);
  }
  
  @Test
  public void testRuleListFailWithRule() {
    FilterContainer filter = new FilterContainer();
    ListenerMessage lm = new ListenerMessage();
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    lm.setChannelType(ChannelType.EPN);
    HashMap<FilterRuleType, FilterRule> ruleMap = new HashMap<FilterRuleType, FilterRule>();
    ruleMap.put(FilterRuleType.PREFETCH, new BaseRuleMock(0));
    ruleMap.put(FilterRuleType.IAB_BOT_LIST, new BaseRuleMock(1));
    filter.put(ChannelType.EPN, ruleMap);
    
    long result= filter.test(lm);
    assertEquals(8, result);
  }
  
  @Test
  public void testFilterContainerFailAllRulesCalled() {
    FilterContainer filter = new FilterContainer();
    ListenerMessage lm = new ListenerMessage();
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    lm.setChannelType(ChannelType.EPN);
    HashMap<FilterRuleType, FilterRule> ruleMap = new HashMap<FilterRuleType, FilterRule>();
    BaseRuleMock rule1 = new BaseRuleMock(1);
    BaseRuleMock rule2 = new BaseRuleMock(1);
    ruleMap.put(FilterRuleType.PREFETCH, rule1);
    ruleMap.put(FilterRuleType.IAB_BOT_LIST, rule2);
    filter.put(ChannelType.EPN, ruleMap);

    long result= filter.test(lm);
    assertEquals(10, result);
    assertEquals(1, rule1.times);
    assertEquals(1, rule2.times);
  }
  
  @Test
  public void testFilterContainerFailSumNotEnough() {
    FilterContainer filter = new FilterContainer();
    ListenerMessage lm = new ListenerMessage();
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    lm.setChannelType(ChannelType.EPN);
    HashMap<FilterRuleType, FilterRule> ruleMap = new HashMap<FilterRuleType, FilterRule>();
    ruleMap.put(FilterRuleType.PREFETCH, new BaseRuleMock(1));
    ruleMap.put(FilterRuleType.IAB_BOT_LIST, new BaseRuleMock(1));
    ruleMap.put(FilterRuleType.EPN_DOMAIN_BLACKLIST, new BaseRuleMock(1));
    filter.put(ChannelType.EPN, ruleMap);
    
    long result= filter.test(lm);
    assertEquals(26, result);
  }
  
  @Test
  public void testFilterContainerBiggestContribution() {
    FilterContainer filter = new FilterContainer();
    ListenerMessage lm = new ListenerMessage();
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    lm.setChannelType(ChannelType.EPN);
    HashMap<FilterRuleType, FilterRule> ruleMap = new HashMap<FilterRuleType, FilterRule>();
    ruleMap.put(FilterRuleType.PREFETCH, new BaseRuleMock(1));
    ruleMap.put(FilterRuleType.IAB_BOT_LIST, new BaseRuleMock(1));
    filter.put(ChannelType.EPN, ruleMap);
    
    long result= filter.test(lm);
    assertEquals(10, result);
  }
  
  @Test
  public void filterRequestTransformImpression() {
    ListenerMessage lm = new ListenerMessage();
    lm.setRequestHeaders("User-Agent: foo|X-Forwarded-For: 127.0.0.1|Referer: https://www.google.com/|X-Purpose:preview||");
    lm.setChannelAction(ChannelAction.IMPRESSION);
    lm.setResponseHeaders("");
    lm.setSnapshotId(12345L);
    lm.setTimestamp(314159L);
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    FilterRequest req = new FilterRequest(lm);
    assertEquals("foo", req.getUserAgent());
    assertEquals("127.0.0.1", req.getSourceIP());
    assertEquals("google.com", req.getReferrerDomain());
    assertEquals(true, req.isPrefetch());
    assertEquals(ChannelAction.IMPRESSION, req.getChannelAction());
  }
  
  @Test
  public void filterRequestTransformClick() {
    ListenerMessage lm = new ListenerMessage();
    lm.setRequestHeaders("X-Forwarded-For: 192.168.0.1|X-EBAY-CLIENT-IP:10.11.12.13|Referer: 100partnerprogramme.de||");
    lm.setChannelAction(ChannelAction.CLICK);
    lm.setResponseHeaders("");
    lm.setSnapshotId(12345L);
    lm.setTimestamp(314159L);
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    FilterRequest req = new FilterRequest(lm);
    assertNull(req.getUserAgent());
    assertEquals("10.11.12.13", req.getSourceIP());
    assertEquals("100partnerprogramme.de", req.getReferrerDomain());
    assertEquals(false, req.isPrefetch());
    assertEquals(ChannelAction.CLICK, req.getChannelAction());
  }
  
  @Test
  public void testFilterRequestResponseCookieParse() {
    ListenerMessage lm = new ListenerMessage();
    lm.setResponseHeaders("Set-Cookie: npii=btrm/svid%3D2649036588643001945a273571^cguid/73eadd911590a93fd3d7362effcb60d85a50cbc9^;Domain=.ebay.co.uk;Expires=Wed, 06-Dec-2017 00:10:25 GMT;Path=/");
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    FilterRequest req = new FilterRequest(lm);
    assertEquals("73eadd911590a93fd3d7362effcb60d8", req.getResponseCGUID());
    assertEquals(1483708489105L, req.getResponseCGUIDTimestamp());
    assertNull(req.getRequestCGUID());
  }
  
  @Test
  public void testFilterRequestURIParse() {
    ListenerMessage lm = new ListenerMessage();
    lm.setCampaignId(5337991765L);
    lm.setUri("http://rover.qa.ebay.com/roverimp/1/705-53470-19255-0/1?campid=5337991765&toolid=20001&customid=link&mpt=606109525");
    FilterRequest req = new FilterRequest(lm);
    assertEquals("705-53470-19255-0", req.getRotationID());
    assertEquals(5337991765L, req.getCampaignId());
  }
  
  @Test
  public void filterRequestURIParseNewFormat() {
    ListenerMessage lm = new ListenerMessage();
    lm.setCampaignId(5337991766L);
    lm.setUri("http://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com");
    FilterRequest req = new FilterRequest(lm);
    assertEquals("1-12345", req.getRotationID());
    assertEquals(5337991766L, req.getCampaignId());
  }
  
  @Test
  public void testFilterRequestRequestCookieParse() {
    ListenerMessage lm = new ListenerMessage();
    lm.setRequestHeaders("Cookie: npii=btguid/7157f9891590abc5e9e24766c9edf1fa5a505d29^trm/svid%3D2716036593043142815a505d29^tpim/1586f29e5^cguid/d30ebafe1580a93d128516d5ffef202f5a505d29^;dp1=bu1p/QEBfX0BAX19AQA**5a505d2a^bl/US5c3190aa^;nonsession=CgAAIABxYlraqMTQ4MzY4MDE3MHgyNzIyNDk1NjcxMzR4MHgyTgDLAAFYbzCyMgDKACBh1SsqNzE1N2Y5ODkxNTkwYWJjNWU5ZTI0NzY2YzllZGYxZmGSq2XR||");
    lm.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
    FilterRequest req = new FilterRequest(lm);
    assertEquals("d30ebafe1580a93d128516d5ffef202f", req.getRequestCGUID());
    assertEquals(1481009707774L, req.getRequestCGUIDTimestamp());
    assertNull(req.getResponseCGUID());
  }
  
  private class BaseRuleMock implements FilterRule {
    public int times = 0;
    private int testResult;
    
    public BaseRuleMock(int result) {
      this.testResult = result;
    }
    
    @Override
    public boolean isChannelActionApplicable(ChannelAction action) { return true; }
    
    public int test(FilterRequest row) {
      ++times;
      return this.testResult;
    }
  }
//
//    @Test
//    public void applicableActions() {
//        FilterRule rule = mock(PrefetchRule.class);
//        when(rule.isChannelActionApplicable(eq(ChannelAction.CLICK))).thenReturn(true);
//        when(rule.isChannelActionApplicable(eq(ChannelAction.IMPRESSION))).thenReturn(false);
//        when(rule.test(any())).thenReturn(0.0f);
//
//        FilterContainer filter = new FilterContainer();
//        filter.put(RuleType.PREFETCH, rule);
//
//        ListenerMessage req1 = new ListenerMessage();
//        req1.setChannelAction(ChannelAction.IMPRESSION);
//        req1.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
//        filter.test(req1);
//
//        verify(rule, times(1)).isChannelActionApplicable(ChannelAction.IMPRESSION);
//        verify(rule, times(0)).test(any());
//
//        ListenerMessage req2 = new ListenerMessage();
//        req2.setChannelAction(ChannelAction.CLICK);
//        req2.setUri("http://rover.qa.ebay.com/rover/1/711-53200-19255-0/1?ff3=4&pub=5575136753&toolid=10001&campid=5337739034");
//        filter.test(req2);
//
//        verify(rule, times(1)).isChannelActionApplicable(ChannelAction.CLICK);
//        verify(rule, times(1)).test(any());
//    }
}
