package chocolate;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.ServiceConfigurationError;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author xiangli4
 */
public class ApplicationOptionsTest {

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(EventListenerServiceTest.class.getClassLoader().getResource
      ("META-INF/configuration/Dev/"));
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

  @Test
  public void testListenerOptions() throws Exception {
    ApplicationOptions.init();
    ApplicationOptions options = ApplicationOptions.getInstance();
    assertEquals("kafka", options.getSinkKafkaCluster());
    assertEquals("dev_listened-paid-search", options.getSinkKafkaConfigs().get(ChannelType.PAID_SEARCH));
    assertEquals("dev_self-service", options.getSelfServiceKafkaTopic());
    assertEquals("com.ebay.traffic.chocolate.kafka.ListenerMessageSerializer", options.getSinkKafkaProperties
      (KafkaCluster.KAFKA).getProperty("value.serializer"));
  }
}
