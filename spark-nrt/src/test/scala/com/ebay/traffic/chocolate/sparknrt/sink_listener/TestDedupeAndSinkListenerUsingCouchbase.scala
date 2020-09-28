//package com.ebay.traffic.chocolate.sparknrt.sink_listener
//
//import com.ebay.app.raptor.chocolate.avro.{ListenerMessage, ChannelAction}
//import com.ebay.traffic.chocolate.common.{KafkaTestHelper, MiniKafkaCluster, TestHelper}
//import com.ebay.traffic.chocolate.kafka.{ListenerMessageDeserializer, ListenerMessageSerializer}
//import com.ebay.traffic.chocolate.spark.BaseFunSuite
//import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
//import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord}
//import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer}
//import com.ebay.traffic.chocolate.sparknrt.couchbase.{CorpCouchbaseClient, CouchbaseClientMock}
//import org.apache.hadoop.fs.Path
//
//class TestDedupeAndSinkListenerUsingCouchbase extends BaseFunSuite {
//  var kafkaCluster: MiniKafkaCluster = null
//
//  val tmpPath = createTempPath()
//  val workDir = tmpPath + "/workDir/"
//  val outputDir = tmpPath + "/outputDir/"
//
//  val topic = "test-kafka-topic"
//
//  val DEDUPE_KEY_PREFIX = "DEDUPE_"
//
//  val channel = "EPN_LISTENER_FILTERED"
//
//  val args = Array(
//    "--mode", "local[8]",
//    "--channel", channel,
//    "--kafkaTopic", topic,
//    "--workDir", workDir,
//    "--outputDir", outputDir,
//    "--couchbaseDedupe", "true"
//  )
//
//  val params = Parameter(args)
//
//  val job = new DedupeAndSinkListener(params)
//
//  var producer: Producer[java.lang.Long, ListenerMessage] = null
//  val callback: Callback = null
//
//  override def beforeAll() = {
//    CouchbaseClientMock.startCouchbaseMock()
//    CorpCouchbaseClient.getBucketFunc = () => {
//      (None, CouchbaseClientMock.connect().openBucket("default"))
//    }
//    kafkaCluster = KafkaTestHelper.newKafkaCluster()
//    producer = kafkaCluster.createProducer[java.lang.Long, ListenerMessage](
//      classOf[LongSerializer], classOf[ListenerMessageSerializer])
//    job.setProperties(kafkaCluster.getConsumerProperties(classOf[LongDeserializer],
//      classOf[ListenerMessageDeserializer]))
//  }
//
//  override def afterAll() = {
//    job.stop()
//    producer.close()
//    KafkaTestHelper.shutdown()
//  }
//
//  def getTimestamp(date: String): Long = {
//    job.sdf.parse(date).getTime
//  }
//
//  def sendListenerMessage(snapshotId: Long, shortSnapshotId: Long, publisherId: Long, campaignId: Long, date: String):
//  ListenerMessage = {
//    val message = TestHelper.newListenerMessage(snapshotId, shortSnapshotId, publisherId, campaignId, getTimestamp(date))
//    message.setChannelAction(ChannelAction.CLICK)
//    val record = new ProducerRecord[java.lang.Long, ListenerMessage](
//      topic, message.getSnapshotId, message)
//    producer.send(record)
//    producer.flush()
//    message
//  }
//
//  test("Test DedupeAndSinkListener") {
//
//    val date1 = "2018-01-01"
//    val date2 = "2018-01-02"
//
//    val DATE_COL1 = job.DATE_COL + "=" + date1
//    val DATE_COL2 = job.DATE_COL + "=" + date2
//
//    val message1 = sendListenerMessage(11L, 1L, 11L, 111L, date1)
//    val message2 = sendListenerMessage(22L, 2L, 22L, 222L, date1)
//    sendListenerMessage(11L, 1L, 11L, 111L, date1) // send duplicate message
//    val message3 = sendListenerMessage(33L, 3L, 33L, 333L, date2)
//    sendListenerMessage(33L, 3L, 33L, 333L, date2) // send duplicate message
//
//    job.run()
//
//    assert(job.fs.exists(new Path(workDir + "last_ts/EPN_LISTENER_FILTERED/0")).equals(true))
//    assert(CorpCouchbaseClient.getBucketFunc()._2.exists(DEDUPE_KEY_PREFIX + "1").equals(true))
//    assert(CorpCouchbaseClient.getBucketFunc()._2.exists(DEDUPE_KEY_PREFIX + "2").equals(true))
//    assert(CorpCouchbaseClient.getBucketFunc()._2.exists(DEDUPE_KEY_PREFIX + "3").equals(true))
//
//    val metadata = Metadata(workDir, channel, MetadataEnum.dedupe)
//    val dom = metadata.readDedupeOutputMeta()
//    assert (dom.length == 1)
//    assert (dom(0)._2.contains(DATE_COL1))
//    assert (dom(0)._2.contains(DATE_COL2))
//
//    val df1 = job.readFilesAsDFEx(dom(0)._2.get(DATE_COL1).get)
//    df1.show()
//    assert (df1.count() == 2)
//
//    val df2 = job.readFilesAsDFEx(dom(0)._2.get(DATE_COL2).get)
//    df2.show()
//    assert (df2.count() == 1)
//
//    metadata.deleteDedupeOutputMeta(dom(0)._1)
//
//    val message4 = sendListenerMessage(44L, 4L, 44L, 444L, date1)
//    val message5 = sendListenerMessage(55L, 5L, 55L, 555L, date2)
//    sendListenerMessage(999L, 1L, 11L, 111L, date1) // duplicated short snapshot id, different snapshot id
//    sendListenerMessage(22L, 2L, 22L, 222L, date1) // send duplicate message
//    sendListenerMessage(33L, 3L, 33L, 333L, date2) // send duplicate message
//
//    job.run()
//
//    val metadata1 = Metadata(workDir, channel, MetadataEnum.dedupe)
//    val dom1 = metadata1.readDedupeOutputMeta()
//    assert (dom1.length == 1)
//    assert (dom1(0)._2.contains(DATE_COL1))
//    assert (dom1(0)._2.contains(DATE_COL2))
//
//    val df11 = job.readFilesAsDFEx(dom1(0)._2.get(DATE_COL1).get)
//    df11.show()
//    assert (df11.count() == 1)
//
//    val df22 = job.readFilesAsDFEx(dom1(0)._2.get(DATE_COL2).get)
//    df22.show()
//    assert (df22.count() == 1)
//
//    metadata.deleteDedupeOutputMeta(dom1(0)._1)
//
//    // empty kafka input
//    job.run()
//
//    val dom2 = metadata1.readDedupeOutputMeta()
//    assert(dom2.length == 0)
//  }
//}