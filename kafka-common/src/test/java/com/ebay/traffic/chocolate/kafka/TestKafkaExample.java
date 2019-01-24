package com.ebay.traffic.chocolate.kafka;

import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;
;import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by yliu29 on 2/27/18.
 */
public class TestKafkaExample {
  static MiniKafkaCluster kafkaCluster;

  @BeforeClass
  public static void setUp() throws IOException {
    kafkaCluster = KafkaTestHelper.newKafkaCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    KafkaTestHelper.shutdown();
  }

  @Test
  public void testKafka() throws Exception {
    Producer<String, String> producer = kafkaCluster.createProducer(
      StringSerializer.class, StringSerializer.class);

    Callback callback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
          e.printStackTrace();
        }
      }
    };

    producer.send(new ProducerRecord<>("test", "key1", "value1"), callback);
    producer.send(new ProducerRecord<>("test", "key2", "value2"), callback);
    producer.close();

    Consumer<String, String> consumer = kafkaCluster.createConsumer(
      StringDeserializer.class, StringDeserializer.class);
    consumer.subscribe(Arrays.asList("test"));

    int count = 0;
    long start = System.currentTimeMillis();
    long end = start;
    while (count < 1 && (end - start < 60 * 1000)) {
      ConsumerRecords<String, String> consumerRecords = consumer.poll(3000);
      Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();

      while (iterator.hasNext()) {
        ConsumerRecord<String, String> record = iterator.next();
        System.out.println(record.key() + ":" + record.value());
        count++;
      }
      end = System.currentTimeMillis();
    }

    Assert.assertTrue(count >= 1);
  }
}
