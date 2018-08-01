package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.ebay.traffic.chocolate.common.TestHelper.*;

/**
 * Created by yliu29 on 3/3/18.
 */
public class TestKafkaProducer {

  private static MiniKafkaCluster kafkaCluster;

  @BeforeClass
  public static void setUp() throws IOException {
    kafkaCluster = KafkaTestHelper.newKafkaCluster();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  @Test
  public void testKafkaProducer() throws Exception {

    final String topic = "marketingtech.ap.tracking-events.filtered-epn";

    Producer<Long, FilterMessage> producer = kafkaCluster.createProducer(
            LongSerializer.class, FilterMessageSerializer.class);

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

    Consumer<Long, FilterMessage> consumer = kafkaCluster.createConsumer(
            LongDeserializer.class, FilterMessageDeserializer.class);

    Map<Long, FilterMessage> messages = pollFromKafkaTopic(
            consumer, Arrays.asList(topic), 3, 60 * 1000);
    consumer.close();

    Assert.assertEquals(messages.size(), 3);
    Assert.assertEquals(1L, messages.get(1L).getSnapshotId().longValue());
    Assert.assertEquals(2L, messages.get(2L).getSnapshotId().longValue());
    Assert.assertEquals(3L, messages.get(3L).getSnapshotId().longValue());
  }
}
