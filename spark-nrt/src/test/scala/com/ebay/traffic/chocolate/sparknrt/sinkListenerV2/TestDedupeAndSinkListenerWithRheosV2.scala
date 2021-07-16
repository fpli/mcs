package com.ebay.traffic.chocolate.sparknrt.sinkListenerV2

import java.util.Properties

import com.ebay.traffic.chocolate.common.TestHelper.loadProperties
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.kafka.clients.producer.Callback

/**
  * Created by zhofan on 5/20/20.
  */
class TestDedupeAndSinkListenerWithRheosV2 extends BaseFunSuite {
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

  val params = ParameterV2(args)

  val job = new DedupeAndSinkListenerV2(params)

  val callback: Callback = null

  override def beforeAll() = {
    val properties: Properties = loadProperties("rheos-kafka-listener-consumer.properties")
    properties.load(getClass.getClassLoader.getResourceAsStream("sherlockio.properties"))
    job.setProperties(properties)
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
