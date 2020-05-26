package com.ebay.traffic.chocolate.sparknrt.sink_listener

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.kafka.clients.producer.Callback

import com.ebay.traffic.chocolate.common.TestHelper.loadProperties

/**
  * Created by zhofan on 5/20/20.
  */
class TestDedupeAndSinkListenerWithRheos extends BaseFunSuite {
  val tmpPath = createTempPath()
  val workDir = tmpPath + "/workDir/"
  val outputDir = tmpPath + "/outputDir/"

  val topic = "marketing.tracking.ssl.listened-filtered"

  val channel = "EPN_LISTENER_FILTERED"

  val couchbaseDedupe = "false"

  val args = Array(
    "--appName", "DedupeAndSinkListener",
    "--mode", "local[8]",
    "--channel", channel,
    "--kafkaTopic", topic,
    "--workDir", workDir,
    "--outputDir", outputDir,
    "--couchbaseDedupe", couchbaseDedupe
  )

  val params = Parameter(args)

  val job = new DedupeAndSinkListener(params)

  val callback: Callback = null

  override def beforeAll() = {
    job.setProperties(loadProperties("rheos-kafka-listener-consumer.properties"))
  }

  override def afterAll() = {
    job.stop()
  }

  def getTimestamp(date: String): Long = {
    job.sdf.parse(date).getTime
  }

  test("Test DedupeAndSinkListener") {
    job.run()
  }
}
