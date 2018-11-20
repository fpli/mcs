package chocolate.util;

import chocolate.EventListenerServiceTest;
import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.listener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.listener.CollectionService;
import com.ebay.app.raptor.chocolate.listener.util.ListenerMessageParser;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.raptor.test.framework.RaptorIOSpringRunner;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import com.ebay.traffic.chocolate.kafka.*;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static com.ebay.traffic.chocolate.common.TestHelper.*;

/**
 * @author xiangli4
 */
@RunWith(RaptorIOSpringRunner.class)
@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = {
    "ginger-client.testService.testClient.endpointUri=http://localhost",
    "ginger-client.testService.testClient.readTimeout=60000"
  },
  classes = EventListenerApplication.class)
public class CollectionServiceTest {

  private static final String ELASTICSEARCH_URL = "chocolate.event-listener.elasticsearch.url";
  private static final String METRICS_INDEX_PREFIX = "chocolate.event-listener.elasticsearch.index.prefix";
  private static MiniKafkaCluster kafkaCluster;

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(EventListenerServiceTest.class.getClassLoader().getResource
      ("META-INF/configuration/Dev/"));
    ApplicationOptions.init();
    ESMetrics.init(ApplicationOptions.getInstance().getByNameString(METRICS_INDEX_PREFIX), ApplicationOptions
      .getInstance().getByNameString(ELASTICSEARCH_URL));
    kafkaCluster = KafkaTestHelper.newKafkaCluster();
    ApplicationOptions options = ApplicationOptions.getInstance();
    options.setSinkKafkaProperties(kafkaCluster.getProducerProperties(
      LongSerializer.class, ListenerMessageSerializer.class));
    KafkaSink.initialize(options);
    ListenerMessageParser.init();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  @Test
  public void testCollect() throws Exception {
    CollectionService cs = CollectionService.getInstance();
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader("X-EBAY-C-ENDUSERCTX", "deviceId=ABCD,deviceIdSource=4PP,appVersion=3.3.0");
    request.addHeader("X-EBAY-C-TRACKING-REF", "guid=0570cd201670a9c422187f22fffee86e5dd496c4," +
      "cguid=0570d8901670a990a825b905ea456fe85dd496c4,tguid=0570cd201670a9c422187f22fffee86e5dd496c4,uid=39787429," +
      "buid=39787429,pageid=3286,cobrandId=2");

    request.setMethod("POST");
    request.setContent("https://www.ebay.com/itm/123456?cid=2".getBytes());
    cs.collect(request);

    request.setContent("https://www.ebay.com/itm/123456?cid=0".getBytes());
    cs.collect(request);

    request.setContent("https://www.ebay.com/itm/123456".getBytes());
    cs.collect(request);

    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
      consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 3, 60 * 1000);
    consumerPaidSearch.close();

    assertEquals(1, listenerMessagesPaidSearch.size());
  }
}
