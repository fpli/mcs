package com.ebay.traffic.chocolate.sparknrt.sink

import com.ebay.app.raptor.chocolate.avro.FilterMessage
import com.ebay.traffic.chocolate.common.{KafkaTestHelper, MiniKafkaCluster, TestHelper}
import com.ebay.traffic.chocolate.kafka.{FilterMessageDeserializer, FilterMessageSerializer}
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.Metadata
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer}

/**
  * Created by yliu29 on 3/12/18.
  */
class TestDedupeAndSink extends BaseFunSuite {
  var kafkaCluster: MiniKafkaCluster = null

  val tmpPath = createTempPath()
  val workDir = tmpPath + "/workDir/"
  val outputDir = tmpPath + "/outputDir/"

  val topic = "test-kafka-topic"

  val args = Array(
    "--mode", "local[8]",
    "--channel", "EPN",
    "--kafkaTopic", topic,
    "--workDir", workDir,
    "--outputDir", outputDir
  )

  val params = Parameter(args)

  val job = new DedupeAndSink(params)

  var producer: Producer[java.lang.Long, FilterMessage] = null
  val callback: Callback = null

  override def beforeAll() = {
    kafkaCluster = KafkaTestHelper.newKafkaCluster()
    producer = kafkaCluster.createProducer[java.lang.Long, FilterMessage](
      classOf[LongSerializer], classOf[FilterMessageSerializer])
    job.setProperties(kafkaCluster.getConsumerProperties(classOf[LongDeserializer],
      classOf[FilterMessageDeserializer]))
  }

  override def afterAll() = {
    job.stop()
    producer.close()
    KafkaTestHelper.shutdown()
  }

  def getTimestamp(date: String): Long = {
    job.sdf.parse(date).getTime
  }

  def sendFilterMessage(snapshotId: Long, publisherId: Long, campaignId: Long, date: String): FilterMessage = {
    val message = TestHelper.newFilterMessage(snapshotId, publisherId, campaignId, getTimestamp(date))
    val record = new ProducerRecord[java.lang.Long, FilterMessage](
      topic, message.getSnapshotId, message)
    producer.send(record)
    producer.flush()
    message
  }

  test("Test Dedupe and Sink") {

    val message1 = sendFilterMessage(1L, 11L, 111L, "2018-01-01")
    val message2 = sendFilterMessage(2L, 22L, 222L, "2018-01-01")
    sendFilterMessage(1L, 11L, 111L, "2018-01-01") // send duplicate message
    val message3 = sendFilterMessage(3L, 33L, 333L, "2018-01-02")
    sendFilterMessage(3L, 33L, 333L, "2018-01-02") // send duplicate message

    job.run()

    val metadata = Metadata(workDir)
    val dom = metadata.readDedupeOutputMeta
    assert (dom.length == 1)
    assert (dom(0)._2.contains("2018-01-01"))
    assert (dom(0)._2.contains("2018-01-02"))

    val df1 = job.readFilesAsDFEx(dom(0)._2.get("2018-01-01").get)
    df1.show()
    assert (df1.count() == 2)

    val df2 = job.readFilesAsDFEx(dom(0)._2.get("2018-01-02").get)
    df2.show()
    assert (df2.count() == 1)

    metadata.deleteDedupeOutputMeta(dom(0)._1)


    val message4 = sendFilterMessage(4L, 44L, 444L, "2018-01-01")
    val message5 = sendFilterMessage(5L, 55L, 555L, "2018-01-02")
    sendFilterMessage(1L, 11L, 111L, "2018-01-01") // send duplicate message
    sendFilterMessage(2L, 22L, 222L, "2018-01-01") // send duplicate message
    sendFilterMessage(3L, 33L, 333L, "2018-01-02") // send duplicate message

    job.run()

    val metadata1 = Metadata(workDir)
    val dom1 = metadata1.readDedupeOutputMeta
    assert (dom1.length == 1)
    assert (dom1(0)._2.contains("2018-01-01"))
    assert (dom1(0)._2.contains("2018-01-02"))

    val df11 = job.readFilesAsDFEx(dom1(0)._2.get("2018-01-01").get)
    df11.show()
    assert (df11.count() == 1)

    val df22 = job.readFilesAsDFEx(dom1(0)._2.get("2018-01-02").get)
    df22.show()
    assert (df22.count() == 1)

    metadata.deleteDedupeOutputMeta(dom1(0)._1)
  }
}
