package chocolate;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.eventlistener.util.Constants;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.raptor.test.framework.RaptorIOSpringRunner;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.ListenerMessageDeserializer;
import com.ebay.traffic.chocolate.kafka.ListenerMessageSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.ebay.traffic.chocolate.common.TestHelper.pollFromKafkaTopic;
import static org.junit.Assert.assertEquals;

/**
 * Created by xiangli4 on 11/19/18.
 * End to End test for Event Listener Service. This class uses Spring test framework to
 * start the test web service, and uses Mini Kafka.
 */
@RunWith(RaptorIOSpringRunner.class)
@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = {
    "ginger-client.testService.testClient.endpointUri=http://localhost",
    "ginger-client.testService.testClient.readTimeout=60000"
  },
  classes = EventListenerApplication.class)
public class EventListenerServiceTest {
  private static MiniKafkaCluster kafkaCluster;

  @LocalServerPort
  private int port;

  private boolean initialized = false;

  private Client client;
  private String svcEndPoint;

  private final String path = "/marketingtracking/v1/events";

  @BeforeClass
  public static void initBeforeTest() throws Exception {
    // inject kafka properties before springboot start
    kafkaCluster = KafkaTestHelper.newKafkaCluster();
    ApplicationOptions options = ApplicationOptions.getInstance();
    options.setSinkKafkaProperties(kafkaCluster.getProducerProperties(
      LongSerializer.class, ListenerMessageSerializer.class));
  }

  @Before
  public void setUp() {
    if (!initialized) {
      RuntimeContext.setConfigRoot(EventListenerServiceTest.class.getClassLoader().getResource
        ("META-INF/configuration/Dev/"));
      Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
      client = ClientBuilder.newClient(configuration);
      String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
      svcEndPoint = endpoint + ":" + port;

      prepareData();
      initialized = true;
    }
  }

  private static void prepareData() {

  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  @Test
  public void testSerivce() throws Exception {

    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setMethod("POST");

    Event event = new Event();
    event.setTargetUrl("https://www.ebay.com/itm/123456?mkevt=1");
    String response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ERROR_NO_USER_AGENT, response);

    request.addHeader("User-Agent", "Desktop");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ERROR_NO_ENDUSERCTX, response);

    request.addHeader("X-EBAY-C-ENDUSERCTX", "deviceId=ABCD,deviceIdSource=4PP,appVersion=3.3.0");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ERROR_NO_TRACKING, response);


    request.addHeader("X-EBAY-C-TRACKING", "cguid=xxx");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ERROR_NO_REFERRER, response);


    event.setReferrer("https://www.google.com");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ACCEPTED, response);

    event.setTargetUrl("https://www.ebay.com/itm/123456?mkevt=1&cid=2");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ACCEPTED, response);

    event.setTargetUrl("https://www.ebay.com/itm/123456?mkevt=1&cid=100");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ACCEPTED, response);

    event.setTargetUrl("https://www.ebay.com/itm/123456?mkevt=1&cid");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ACCEPTED, response);

    event.setTargetUrl("https://www.ebay.com/itm/123456?mkevt=0");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ERROR_INVALID_MKEVT, response);

    event.setTargetUrl("https://www.ebay.com/itm/123456");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ERROR_NO_QUERY_PARAMETER, response);

    event.setTargetUrl("https://www.ebay.com/itm/123456?abc=123");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ERROR_NO_MKEVT, response);

    request.addHeader("User-Agent", "Mobile");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&cid=2");
    response = CollectionService.getInstance().collect(request, event);
    assertEquals(Constants.ACCEPTED, response);

    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
      consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 4, 5 * 1000);
    consumerPaidSearch.close();

    assertEquals(2, listenerMessagesPaidSearch.size());
  }
}
