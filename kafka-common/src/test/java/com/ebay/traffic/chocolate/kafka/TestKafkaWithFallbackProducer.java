package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static com.ebay.traffic.chocolate.common.TestHelper.newFilterMessage;
import static com.ebay.traffic.chocolate.common.TestHelper.pollFromKafkaTopic;

/**
 * Created by yliu29 on 2/13/18.
 *
 */
public class TestKafkaWithFallbackProducer {

  private static MiniKafkaCluster cluster1;
  private static MiniKafkaCluster cluster2;

  @BeforeClass
  public static void setUp() throws IOException {
    cluster1 = KafkaTestHelper.newKafkaCluster();
    cluster2 = new MiniKafkaCluster();
    cluster2.start();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
    cluster2.shutdown();
  }

  @Test
  public void testKafkaWithFallbackProducer() throws Exception {

    final String topic1 = "testtopic1";
    final String topic2 = "testtopic2";
    final String topic = topic1 + KafkaCluster.DELIMITER + topic2;

    Properties properties1 = cluster1.getProducerProperties(
            LongSerializer.class, FilterMessageSerializer.class);
    properties1.setProperty("max.block.ms", "1");
    Producer<Long, FilterMessage> producer1 = new KafkaProducer<>(properties1);

    Producer<Long, FilterMessage> producer2 = cluster2.createProducer(
            LongSerializer.class, FilterMessageSerializer.class);

    Producer<Long, FilterMessage> producer = new KafkaWithFallbackProducer<>(producer1, producer2);

    FilterMessage message1 = newFilterMessage(1L, 11L, 111L);
    FilterMessage message2 = newFilterMessage(2L, 22L, 222L);
    FilterMessage message3 = newFilterMessage(3L, 33L, 333L);

    Callback callback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
          e.printStackTrace();
        }
      }
    };

    producer.send(new ProducerRecord<>(topic, 1L, message1), callback);
    producer.send(new ProducerRecord<>(topic, 3L, message3), callback);
    producer.send(new ProducerRecord<>(topic, 2L, message2), callback);
    producer.flush();
    producer.close();

    Consumer<Long, FilterMessage> consumer = cluster2.createConsumer(
            LongDeserializer.class, FilterMessageDeserializer.class);

    Map<Long, FilterMessage> messages = pollFromKafkaTopic(
            consumer, Arrays.asList(topic2), 3, 10 * 1000);
    consumer.close();

    Assert.assertEquals(messages.size(), 3);
    Assert.assertEquals(1L, messages.get(1L).getSnapshotId().longValue());
    Assert.assertEquals(2L, messages.get(2L).getSnapshotId().longValue());
    Assert.assertEquals(3L, messages.get(3L).getSnapshotId().longValue());
  }
}
