package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.kernel.context.RuntimeContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceConfigurationError;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author jepounds
 *
 * TODO: this test class can be removed.
 */
public class ApplicationOptionsTest {

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(FilterServiceTest.class.getClassLoader().getResource("META-INF/configuration/Dev/"));
  }
  
  @Test
  public void testImmediateOptions() {
    Properties prop = new Properties();
    prop.put("foo", "value");
    prop.put("bar", "true");
    prop.put("baz", "-1234");
    prop.put("qizzy", "123456789");
    prop.put("zyx", "-12.34");
    ApplicationOptions.init(prop);
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertEquals("value", options.getByNameString("foo"));
    assertEquals(true, options.getByNameBoolean("bar"));
    assertEquals(-1234, (int) options.getByNameInteger("baz"));
    assertEquals(123456789L, (long) options.getByNameLong("qizzy"));
    assertEquals(-12.34, options.getByNameFloat("zyx"), 1e-5);
  }
  
  @Test(expected = ServiceConfigurationError.class)
  public void testImmediateOptionsFail1() {
    Properties prop = new Properties();
    ApplicationOptions.init(prop);
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertNull(options.getByNameString("qux"));
  }
  
  @Test(expected = ServiceConfigurationError.class)
  public void testImmediateOptionsFail2() {
    Properties prop = new Properties();
    ApplicationOptions.init(prop);
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertNull(options.getByNameBoolean("qux"));
  }
  
  @Test(expected = ServiceConfigurationError.class)
  public void testImmediateOptionsFail3() {
    Properties prop = new Properties();
    ApplicationOptions.init(prop);
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertNull(options.getByNameInteger("qux"));
  }
  
  @Test(expected = ServiceConfigurationError.class)
  public void testImmediateOptionsFail4() {
    Properties prop = new Properties();
    ApplicationOptions.init(prop);
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertNull(options.getByNameLong("qux"));
  }
  
  @Test(expected = ServiceConfigurationError.class)
  public void testImmediateOptionsFail5() {
    Properties prop = new Properties();
    ApplicationOptions.init(prop);
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertNull(options.getByNameFloat("qux"));
  }
  
  @Test
  public void testZookeeperConnect() {
    Properties prop = new Properties();
    String zkConnect = "127.1:2181";
    prop.put(ApplicationOptions.ZK_CONNECT_PROPERTY, zkConnect);
    ApplicationOptions.init(prop);
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertEquals(options.getZookeeperString(), zkConnect);
  }
  
  @Test
  public void testInitFilterRuleConfig() throws IOException {
    ApplicationOptions.initFilterRuleConfig("filter_rule_config.json");
    Map<String, FilterRuleContent> defaultRule = ApplicationOptions.filterRuleConfigMap.get(ChannelType.DEFAULT);
    assertEquals("PrefetchRule", defaultRule.get("PrefetchRule").getRuleName());
    assertEquals(1, defaultRule.get("PrefetchRule").getRuleWeight(), 0.001);
    
    Map<String, FilterRuleContent> epnRule = ApplicationOptions.filterRuleConfigMap.get(ChannelType.EPN);
    assertEquals("PrefetchRule", epnRule.get("PrefetchRule").getRuleName());
    assertEquals(1, epnRule.get("PrefetchRule").getRuleWeight(), 0.001);
    assertEquals("EPNDomainBlacklistRule", epnRule.get("EPNDomainBlacklistRule").getRuleName());
    assertEquals(0.5, epnRule.get("EPNDomainBlacklistRule").getRuleWeight(), 0.001);
    
    Map<String, FilterRuleContent> dapRule = ApplicationOptions.filterRuleConfigMap.get(ChannelType.DISPLAY);
    assertEquals("TwoPassIABRule", dapRule.get("TwoPassIABRule").getRuleName());
    assertEquals(1, dapRule.get("TwoPassIABRule").getRuleWeight(), 0.001);
    assertEquals("IPBlacklistRule", dapRule.get("IPBlacklistRule").getRuleName());
    assertEquals(0, dapRule.get("IPBlacklistRule").getRuleWeight(), 0.001);
  }
}
