package com.ebay.traffic.chocolate.sparknrt.sword

import java.util.Properties

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class SwordJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  //  lazy val dataDir = "/chocolate/spark-nrt/out/"
  lazy val dataDir: String = params.dataDir + params.channel + "/capping/"

  @transient lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum("capping")
    Metadata(params.workDir, params.channel, usage)
  }

  @transient lazy val kafkaProducer: KafkaProducer[Long, Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", params.bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[Long, Array[Byte]](props)
  }

  import spark.implicits._

  override def run(): Unit = {
    var dedupeOutputMeta = inputMetadata.readDedupeOutputMeta(".detection")
    // at most 3 meta files
    if (dedupeOutputMeta.length > 3) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, 3)
    }
    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        val df = readFilesAsDFEx(datesFile._2)
          .filter($"channel_action" === "CLICK")
        df.foreach(one => {
          kafkaProducer.send(new ProducerRecord[Long, Array[Byte]](
            params.kafkaTopic, one.getLong(0),
            scala.util.parsing.json.JSONObject(one.getValuesMap(one.schema.fieldNames)).toString().getBytes("UTF-8")))
        })
      })
      inputMetadata.deleteDedupeOutputMeta(file)
      logger.info(s"processed:$file")
    })
    kafkaProducer.flush()
    kafkaProducer.close()

  }

}