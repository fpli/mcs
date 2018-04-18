//package com.ebay.traffic.chocolate.kafka;
//
//import com.ebay.app.raptor.chocolate.avro.FilterMessage;
//import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.*;
//import org.junit.*;
//
//import java.util.Arrays;
//import java.util.Iterator;
//
//import static com.ebay.traffic.chocolate.common.TestHelper.*;
//
///**
// * Created by yliu29 on 2/14/18.
// */
//public class TestRheosKafkaProducer {
//
//  @Test
//  public void testRheosKafkaProducer() throws Exception {
//    final String topic = "marketingtech.ap.tracking-events.filtered-epn";
//
//    Producer<Long, FilterMessage> producer =
//            new RheosKafkaProducer<>(loadProperties("rheos-kafka-filter-producer.properties"));
//
//    FilterMessage message1 = newFilterMessage(1L, 11L, 111L);
//    FilterMessage message2 = newFilterMessage(2L, 22L, 222L);
//    FilterMessage message3 = newFilterMessage(3L, 33L, 333L);
//
//    Callback callback = new Callback() {
//      @Override
//      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//        if (e != null) {
//          e.printStackTrace();
//        }
//      }
//    };
//
//    producer.send(new ProducerRecord<>(topic, 1L, message1), callback);
//    producer.send(new ProducerRecord<>(topic, 3L, message3), callback);
//    producer.send(new ProducerRecord<>(topic, 2L, message2), callback);
//    producer.flush();
//    producer.close();
//
//    System.out.println("Producer sent 3 message.");
//
//    Consumer<Long, FilterMessage> consumer =
//            new KafkaConsumer<>(loadProperties("rheos-kafka-filter-consumer.properties"));
//    consumer.subscribe(Arrays.asList(topic));
//
//    int count = 0;
//    long start = System.currentTimeMillis();
//    long end = start;
//    while (count < 1 && (end - start < 3 * 60 * 1000)) {
//      ConsumerRecords<Long, FilterMessage> consumerRecords = consumer.poll(3000);
//      Iterator<ConsumerRecord<Long, FilterMessage>> iterator = consumerRecords.iterator();
//
//      while (iterator.hasNext()) {
//        ConsumerRecord<Long, FilterMessage> record = iterator.next();
//        FilterMessage message = record.value();
//        System.out.println(message);
//        count++;
//      }
//      end = System.currentTimeMillis();
//    }
//
//    Assert.assertTrue(count >= 1);
//  }
//}
