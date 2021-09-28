package com.ebay.traffic.chocolate.sparknrt.sink

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.kafka.clients.producer.Callback

import com.ebay.traffic.chocolate.common.TestHelper.loadProperties

/**
  * Created by jialili1 on 5/15/19.
  */
class TestDedupeAndSinkWithRheos extends BaseFunSuite {

  val tmpPath = createTempPath()
  val workDir = tmpPath + "/workDir/"
  val outputDir = tmpPath + "/outputDir/"

  val topic = "marketing.tracking.ssl.filtered-epn"

  val channel = "EPN"

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
    "--kafkaTopic", topic,
    "--workDir", workDir,
    "--outputDir", outputDir
  )

  val params = Parameter(args)

  val job = new DedupeAndSink(params)

  val callback: Callback = null

  override def beforeAll() = {
    job.setProperties(loadProperties("rheos-kafka-consumer.properties"))
  }

  override def afterAll() = {
    job.stop()
  }

  def getTimestamp(date: String): Long = {
    job.sdf.parse(date).getTime
  }

  test("Test Dedupe and Sink") {
    job.run()
  }
}
