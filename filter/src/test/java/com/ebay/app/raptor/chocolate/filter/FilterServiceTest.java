package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import com.ebay.traffic.chocolate.common.MiniZookeeperCluster;
import com.ebay.traffic.chocolate.kafka.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.ebay.traffic.chocolate.common.TestHelper.*;

/**
 * Created by yliu29 on 3/3/18.
 *
 * End to End test for Filter Service. This class uses Spring test framework to
 * start the test web service, and uses Mini Kafka and Zookeeper cluster.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = { "GingerClient.testService.testClient.endpointUri=http://localhost",
    "GingerClient.testService.testClient.readTimeout=5000"})
public class FilterServiceTest {
  private static MiniKafkaCluster kafkaCluster;
  private static MiniZookeeperCluster zookeeperCluster;

  @BeforeClass
  public static void setUp() throws IOException {
//    RuntimeContext.setConfigRoot(FilterServiceTest.class.
//            getClassLoader().getResource("META-INF/configuration/Dev/"));
    kafkaCluster = KafkaTestHelper.newKafkaCluster();
    zookeeperCluster = kafkaCluster.getZookeeper();
    ApplicationOptions options = ApplicationOptions.getInstance();
    options.setInputKafkaProperties(kafkaCluster.getConsumerProperties(
            LongDeserializer.class, ListenerMessageDeserializer.class));
    options.setSinkKafkaProperties(kafkaCluster.getProducerProperties(
            LongSerializer.class, FilterMessageSerializer.class));
    options.setZkConnectionString(zookeeperCluster.getConnectionString());
    options.setZkConnectionStringForPublisherCache(zookeeperCluster.getConnectionString());

    prepareData();
  }

  private static void prepareData() throws IOException {
    Producer<Long, ListenerMessage> producer = kafkaCluster.createProducer(
            LongSerializer.class, ListenerMessageSerializer.class);

    ListenerMessage ePNmessage1 = newListenerMessage(
            ChannelType.EPN, ChannelAction.CLICK, 1L, 11L, 111L);
    ListenerMessage ePNmessage2 = newListenerMessage(
            ChannelType.EPN, ChannelAction.CLICK, 2L, 22L, 333L);
    ListenerMessage ePNmessage3 = newListenerMessage(
            ChannelType.EPN, ChannelAction.CLICK, 3L, 33L, 333L);
    producer.send(new ProducerRecord<>("dev_listener", 1L, ePNmessage1));
    producer.send(new ProducerRecord<>("dev_listener", 2L, ePNmessage2));
    producer.send(new ProducerRecord<>("dev_listener", 3L, ePNmessage3));

    ListenerMessage displayMessage1 = newListenerMessage(
            ChannelType.DISPLAY, ChannelAction.CLICK, 1L, 11L, 111L);
    ListenerMessage displayMessage2 = newListenerMessage(
            ChannelType.DISPLAY, ChannelAction.IMPRESSION, 2L, 22L, 333L);

    producer.send(new ProducerRecord<>("dev_listener_display", 1L, displayMessage1));
    producer.send(new ProducerRecord<>("dev_listener_display", 2L, displayMessage2));

    ListenerMessage roiMessage = newROIMessage(
        ChannelType.ROI, ChannelAction.ROI, 1L, -1L, -1L);
    producer.send(new ProducerRecord<>("dev_listener", 4L, roiMessage));

    producer.flush();
    producer.close();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  /**
   * This test sends click, imp, vimp events to kafka topics for listener, and verify the output
   * topics for filter in kafka cluster after filter processes them.
   */
  @Test
  public void testFilterService() throws Exception {

    // wait few seconds to let Filter worker threads run first.
    Thread.sleep(3000);

    KafkaSink.get().flush();

    Consumer<Long, FilterMessage> consumerEPN = kafkaCluster.createConsumer(
            LongDeserializer.class, FilterMessageDeserializer.class);
    Map<Long, FilterMessage> filterMessagesEPN = pollFromKafkaTopic(
            consumerEPN, Arrays.asList("dev_filter"), 3, 60 * 1000);
    consumerEPN.close();

    Assert.assertEquals(3, filterMessagesEPN.size());
    Assert.assertEquals(1L, filterMessagesEPN.get(1L).getSnapshotId().longValue());
    Assert.assertEquals(2L, filterMessagesEPN.get(2L).getSnapshotId().longValue());
    Assert.assertEquals(3L, filterMessagesEPN.get(3L).getSnapshotId().longValue());

    Consumer<Long, FilterMessage> consumerDisplay = kafkaCluster.createConsumer(
            LongDeserializer.class, FilterMessageDeserializer.class);

    Map<Long, FilterMessage> filterMessagesDisplay = pollFromKafkaTopic(
            consumerDisplay, Arrays.asList("dev_filter_display"), 2, 60 * 1000);
    consumerDisplay.close();

    Assert.assertEquals(2, filterMessagesDisplay.size());
    Assert.assertEquals(1L, filterMessagesDisplay.get(1L).getSnapshotId().longValue());
    Assert.assertEquals(2L, filterMessagesDisplay.get(2L).getSnapshotId().longValue());
    Assert.assertEquals(ChannelAction.IMPRESSION, filterMessagesDisplay.get(2L).getChannelAction());

    Consumer<Long, FilterMessage> consumerROI = kafkaCluster.createConsumer(
        LongDeserializer.class, FilterMessageDeserializer.class);

    Map<Long, FilterMessage> filterMessagesROI = pollFromKafkaTopic(
        consumerROI, Arrays.asList("dev_filter_roi"), 1, 60 * 1000);
    consumerROI.close();

    Assert.assertEquals(1, filterMessagesROI.size());
    Assert.assertEquals(1L, filterMessagesROI.get(1L).getSnapshotId().longValue());
    Assert.assertEquals(ChannelAction.ROI, filterMessagesROI.get(1L).getChannelAction());
  }
}
