package com.ebay.traffic.chocolate.listener;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.raptor.test.framework.RaptorIOSpringRunner;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import com.ebay.traffic.chocolate.common.MiniZookeeperCluster;
import com.ebay.traffic.chocolate.kafka.ListenerMessageDeserializer;
import com.ebay.traffic.chocolate.kafka.ListenerMessageSerializer;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.ebay.traffic.chocolate.common.TestHelper.pollFromKafkaTopic;

/**
 * Created by yliu29 on 3/4/18.
 *
 * End to End test for Listener Service. This class uses Spring test framework to
 * start the test web service, and uses Mini Kafka and Zookeeper cluster.
 */
@RunWith(RaptorIOSpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ListenerServiceTest {

//  private static URL listenerConfigs = ListenerServiceTest.
//          class.getClassLoader().getResource("META-INF/configuration/Dev/chocolate-listener.xml");
//
//  static {
//    System.setProperty("chocolate-listener.xml", listenerConfigs.toString());
//  }

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  private static MiniKafkaCluster kafkaCluster;
  private static MiniZookeeperCluster zookeeperCluster;

  @BeforeClass
  public static void setUp() throws IOException {
//    RuntimeContext.setConfigRoot(ListenerServiceTest.class.
//            getClassLoader().getResource("META-INF/configuration/Dev/"));
    //ListenerOptions.init(listenerConfigs.openStream());
    kafkaCluster = KafkaTestHelper.newKafkaCluster();
    zookeeperCluster = kafkaCluster.getZookeeper();
    ListenerOptions options = ListenerOptions.getInstance();
    options.setKafkaProperties(kafkaCluster.getProducerProperties(
            LongSerializer.class, ListenerMessageSerializer.class));
    //ListenerInitializer.init(options);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  /**
   * This test sends click, imp, vimp events to listener, and verify the output
   * in kafka cluster after listener processes them.
   */
  @Test
  public void testFilterService() throws Exception {
    String page = "http://www.ebay.com/itm/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-/380963112068";
    String clickURL = "/1c/1-12345?page=" + page + "&item=380963112068";
    String vimpURL = "/1v/1-12345?page=" + page + "&item=380963112068";
    String impURL = "/1i/1-12345?page=" + page + "&item=380963112068";


    Assert.assertEquals(HttpStatus.FOUND, restTemplate.getForEntity(
            "http://127.0.0.1:" + port + clickURL, String.class).getStatusCode());
    Assert.assertEquals(HttpStatus.OK, restTemplate.getForEntity(
            "http://127.0.0.1:" + port + vimpURL, String.class).getStatusCode());
    Assert.assertEquals(HttpStatus.OK, restTemplate.getForEntity(
            "http://127.0.0.1:" + port + impURL, String.class).getStatusCode());

    // wait few seconds to let Listener worker threads run first.
    Thread.sleep(2000);

    Consumer<Long, ListenerMessage> consumer = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessages = pollFromKafkaTopic(
            consumer, Arrays.asList("dev_listener"), 3, 60 * 1000);
    consumer.close();

    Assert.assertEquals(3, listenerMessages.size());
    boolean hasClick = false;
    boolean hasVimp = false;
    boolean hasImp = false;
    for (ListenerMessage message : listenerMessages.values()) {
      if (message.getChannelAction() == ChannelAction.CLICK) {
        hasClick = true;
      } else if (message.getChannelAction() == ChannelAction.IMPRESSION) {
        hasImp = true;
      } else if (message.getChannelAction() == ChannelAction.VIEWABLE) {
        hasVimp = true;
      }
    }
    Assert.assertTrue(hasClick && hasVimp && hasImp);
  }

  @Test
  public void testPostMethodSuccessfully() throws Exception{
    String page = "http://www.ebay.com/itm/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-/380963112068";
    String clickURL = "http://127.0.0.1:" + port + "/1c/1-12345";

    HttpHeaders headers = new HttpHeaders();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("page", page);
    HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(map, headers);
    ResponseEntity<String> response = restTemplate.postForEntity(clickURL, request, String.class);
    Assert.assertEquals(HttpStatus.FOUND, response.getStatusCode());

    // wait few seconds to let Listener worker threads run first.
    Thread.sleep(2000);

    Consumer<Long, ListenerMessage> consumer = kafkaCluster.createConsumer(
        LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessages = pollFromKafkaTopic(
        consumer, Arrays.asList("dev_listener"), 1, 60 * 1000);
    consumer.close();

    Assert.assertEquals(1, listenerMessages.size());
  }
}
