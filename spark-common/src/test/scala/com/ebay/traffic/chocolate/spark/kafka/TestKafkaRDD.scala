package com.ebay.traffic.chocolate.spark.kafka

import java.text.SimpleDateFormat
import java.util.Date

import com.ebay.app.raptor.chocolate.avro.FilterMessage
import com.ebay.traffic.chocolate.common.{KafkaTestHelper, MiniKafkaCluster, TestHelper}
import com.ebay.traffic.chocolate.kafka.{FilterMessageDeserializer, FilterMessageSerializer}
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yliu29 on 3/12/18.
  */
class TestKafkaRDD extends BaseFunSuite {
  var kafkaCluster: MiniKafkaCluster = null

  lazy val conf = {
    new SparkConf().setAppName("test").setMaster("local[8]").set("spark.driver.bindAddress", "127.0.0.1")
  }

  lazy val sc = {
    new SparkContext(conf)
  }

  override def beforeAll() = {
    kafkaCluster = KafkaTestHelper.newKafkaCluster()
  }

  override def afterAll() = {
    sc.stop()
    KafkaTestHelper.shutdown()
  }

  test("Test Kafka RDD") {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    println(sdf.format(new Date(System.currentTimeMillis())))

    val topic = "test-kafka-topic"

    val producer = kafkaCluster.createProducer[java.lang.Long, FilterMessage](
      classOf[LongSerializer], classOf[FilterMessageSerializer])
    val callback: Callback = null

    val message1 = TestHelper.newFilterMessage(1L, 11L, 111L)
    val message2 = TestHelper.newFilterMessage(2L, 22L, 222L)
    val message3 = TestHelper.newFilterMessage(3L, 33L, 333L)
    val record1 = new ProducerRecord[java.lang.Long, FilterMessage](
      topic, message1.getSnapshotId, message1)
    val record2 = new ProducerRecord[java.lang.Long, FilterMessage](
      topic, message2.getSnapshotId, message2)
    val record3 = new ProducerRecord[java.lang.Long, FilterMessage](
      topic, message3.getSnapshotId, message3)

    producer.send(record1)
    producer.send(record2)
    producer.send(record3)
    producer.flush()

    // consume the data using KafkaRDD, and do not commit offsets
    assertKafkaRDD(topic, false, Map(message1.getSnapshotId -> message1,
      message2.getSnapshotId -> message2, message3.getSnapshotId -> message3))

    // consume the data using kafkaRDD, and commit offsets
    // Should be able to consume all records since offsets was not committed.
    assertKafkaRDD(topic, true, Map(message1.getSnapshotId -> message1,
      message2.getSnapshotId -> message2, message3.getSnapshotId -> message3))

    // consume the data using kafkaRDD, and commit offsets
    // Should not be able to consume any record since offsets was committed
    assertKafkaRDD(topic, true, Map())

    val message4 = TestHelper.newFilterMessage(4L, 44L, 444L)
    val message5 = TestHelper.newFilterMessage(5L, 55L, 555L)
    val record4 = new ProducerRecord[java.lang.Long, FilterMessage](
      topic, message4.getSnapshotId, message4)
    val record5 = new ProducerRecord[java.lang.Long, FilterMessage](
      topic, message5.getSnapshotId, message5)
    producer.send(record4)
    producer.send(record5)
    producer.flush()

    // consume the data using KafkaRDD, and commit offsets
    // Should be able to consume records since there are more records
    assertKafkaRDD(topic, true, Map(message4.getSnapshotId -> message4,
      message5.getSnapshotId -> message5))

    // consume the data using kafkaRDD, and commit offsets
    // Should not be able to consume any record since offsets was committed
    assertKafkaRDD(topic, true, Map())

    producer.close()
  }

  def assertKafkaRDD(topic: String, commit: Boolean, expected: Map[java.lang.Long, FilterMessage]) = {
    val kafkaRDD = new KafkaRDD[java.lang.Long, FilterMessage](
      sc, topic, kafkaCluster.getConsumerProperties(classOf[LongDeserializer], classOf[FilterMessageDeserializer]))

    val result = kafkaRDD.map(record => (record.key(), record.value().writeToJSON())).collect()
    if (commit) {
      kafkaRDD.commitOffsets()
    }
    kafkaRDD.close()
    assert(result.length == expected.size)

    result.foreach(kv => {
      assert(expected.get(kv._1).get == FilterMessage.readFromJSON(kv._2))
    })
  }
}
