package chocolate;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.listener.ApplicationOptions;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.raptor.test.framework.RaptorIOSpringRunner;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import com.ebay.traffic.chocolate.kafka.ListenerMessageSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

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

  private static final String helloPath = "/marketingtracking/v1/events/hello";

  @Before
  public void setUp() throws IOException {

    if (!initialized) {
      Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
      client = ClientBuilder.newClient(configuration);
      String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
      svcEndPoint = endpoint + ":" + port;
      kafkaCluster = KafkaTestHelper.newKafkaCluster();
      ApplicationOptions options = ApplicationOptions.getInstance();
      options.setSinkKafkaProperties(kafkaCluster.getProducerProperties(
        LongSerializer.class, ListenerMessageSerializer.class));

      prepareData();
      initialized = true;
    }
  }

  private static void prepareData() throws IOException {

  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  @Test
  public void testHello() {
    Response result = client.target(svcEndPoint).path(helloPath).request().accept(MediaType.APPLICATION_JSON_TYPE).get();
    assertEquals(200, result.getStatus());
    assertEquals("Hello from Raptor IO", result.readEntity(String.class));
  }
//
//  @Test
//  public void testFilterService() throws Exception {
//
//    // wait few seconds to let Filter worker threads run first.
//    Thread.sleep(3000);
//
//    KafkaSink.get().flush();
//
//    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
//      LongDeserializer.class, ListenerMessageDeserializer.class);
//    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
//      consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 3, 60 * 1000);
//    consumerPaidSearch.close();
//
//    Assert.assertEquals(3, listenerMessagesPaidSearch.size());
//    Assert.assertEquals(1L, listenerMessagesPaidSearch.get(1L).getSnapshotId().longValue());
//    Assert.assertEquals(2L, listenerMessagesPaidSearch.get(2L).getSnapshotId().longValue());
//    Assert.assertEquals(3L, listenerMessagesPaidSearch.get(3L).getSnapshotId().longValue());
//  }
}
