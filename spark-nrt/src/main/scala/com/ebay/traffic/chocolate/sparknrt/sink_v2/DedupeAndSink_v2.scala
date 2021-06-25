package com.ebay.traffic.chocolate.sparknrt.sink_v2

import java.security.SecureRandom
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.{lang, util}

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, FilterMessage}
import com.ebay.traffic.chocolate.spark.kafka.KafkaRDD_v2
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase_v2.CorpCouchbaseClient_v2
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.monitoring.Field
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics
import org.apache.avro.generic.GenericRecord
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.TaskContext
import rx.Observable
import rx.functions.Func1

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Created by yuhxiao on 23/02/21.
  */
object DedupeAndSink_v2 extends App {
  override def main(args: Array[String]) = {
    val params = Parameter_v2(args)

    val job = new DedupeAndSink_v2(params)

    job.run()
    job.stop()
  }
}

class DedupeAndSink_v2(params: Parameter_v2)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("kafka_v2.properties"))
    properties.load(getClass.getClassLoader.getResourceAsStream("sherlockio.properties"))
    properties
  }

  @transient var jobProperties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("dedupe_and_sink_v2.properties"))
    properties
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def getDateString(timestamp: Long): String = {
    sdf.format(new Date(timestamp))
  }

  @transient lazy val rand = {
    new SecureRandom()
  }

  lazy val baseDir = params.workDir + "/dedupe/" + params.channel + "/"
  lazy val lagDir = params.workDir + "/last_ts/" + params.channel + "/"
  lazy val baseTempDir = baseDir + "/tmp/"
  lazy val sparkDir = baseDir + "/spark/"
  lazy val outputDir = params.outputDir + "/" + params.channel + "/dedupe/"
  var couchbaseDedupe = params.couchbaseDedupe
  lazy val couchbaseTTL = params.couchbaseTTL
  lazy val DEDUPE_KEY_PREFIX = "DEDUPE_"
  lazy val CHANNEL_ACTION = "channelAction"
  lazy val CHANNEL_TYPE = "channelType"

  @transient lazy val metadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.dedupe)
  }

  val SHORT_SNAPSHOT_ID_COL = "short_snapshot_id"

  @transient lazy val metrics: SherlockioMetrics = {
    SherlockioMetrics.init(properties.getProperty("sherlockio.namespace"),properties.getProperty("sherlockio.endpoint"),properties.getProperty("sherlockio.user"))
    val sherlockioMetrics = SherlockioMetrics.getInstance()
    sherlockioMetrics.setJobName(params.appName)
    sherlockioMetrics
  }

  import spark.implicits._

  override def run() = {

    logger.info("baseDir: " + baseDir)
    logger.info("outputDir: " + outputDir)

    // clean base dir
    fs.delete(new Path(baseDir), true)
    fs.mkdirs(new Path(baseDir))
    fs.mkdirs(new Path(baseTempDir))
    fs.mkdirs(new Path(sparkDir))

    val suffix = jobProperties.getProperty("meta.output.suffix")
    var suffixArray: Array[String] = Array()
    if (StringUtils.isNotEmpty(suffix)) {
      suffixArray = suffix.split(",")
    }

    val kafkaRDD = new KafkaRDD_v2[lang.Long, FilterMessage](
      sc, params.kafkaTopic, properties, params.appName, params.maxConsumeSize)

    val dates =
    kafkaRDD.mapPartitions(iter => {
      val files = new util.HashMap[String, String]()
      val writers = new util.HashMap[String, ParquetWriter[GenericRecord]]()
      var hasWrittenTimestamp = false

      while (iter.hasNext) {
        val message = iter.next().value()
        metric(message)
        if (!hasWrittenTimestamp) {
          writePartitionLag(message)
          hasWrittenTimestamp = true
        }
        val date = DATE_COL + "=" + getDateString(message.getTimestamp) // get the event date
        var writer = writers.get(date)
        if (writer == null) {
          val file = "/" + date + "/" + Math.abs(rand.nextLong()) + ".parquet"
          files.put(date, file)

          logger.info("Create AvroParquetWriter for path: " + baseTempDir + file)

          writer = AvroParquetWriter.
            builder[GenericRecord](new Path(baseTempDir + file))
            .withSchema(FilterMessage.getClassSchema())
            .withConf(hadoopConf)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()

          writers.put(date, writer)
        }
        // write message
        dedupeByCBAndWriteMessage(message, writer)
      }

      flushMetric

      // 1. close the parquet writers
      // 2. rename tmp files to final files
      val iterator = writers.entrySet().iterator()

      val dates = new ArrayBuffer[String]()
      while (iterator.hasNext) {
        val output = iterator.next()
        val date = output.getKey
        val writer = output.getValue
        writer.close() // close the parquet writer
        val file = files.get(date)
        // rename tmp files to final files
        fs.mkdirs(new Path(baseDir + "/" + date))
        fs.rename(new Path(baseTempDir + file), new Path(baseDir + file))
        logger.info("Rename after writing parquet file, from: " + baseTempDir + file + " to: " + baseDir + file)
        dates += date
      }

      dates.iterator
    }).distinct().collect()

    logger.info("dedupe output date: " + dates.mkString(","))

    // delete the tmp dir
    fs.delete(new Path(baseTempDir), true)

    // dedupe
    var metaFiles: MetaFiles = null
    if(dates.length>0) {
      metaFiles = new MetaFiles(dates.map(date => dedupeThisAndLastMeta(date)))

      metadata.writeDedupeCompMeta(metaFiles)
      metadata.writeDedupeOutputMeta(metaFiles, suffixArray)
    }
    // commit offsets of kafka RDDs
    kafkaRDD.commitOffsets()
    kafkaRDD.close()

    writeSnapshort2Cb(metaFiles)

    // delete the dir
    fs.delete(new Path(baseDir), true)
  }

  /**
    * Dedupe by couchbase to click and roi. Write result to file.
    * @param message filter message
    * @param writer file writer
    */
  private def dedupeByCBAndWriteMessage(message: FilterMessage, writer: ParquetWriter[GenericRecord]) = {
    // couchbase dedupe only apply on click
    if ((message.getChannelAction == ChannelAction.CLICK || message.getChannelAction == ChannelAction.ROI) && couchbaseDedupe) {
      try {
        val (cacheClient, bucket) = CorpCouchbaseClient_v2.getBucketFunc()
        val key = DEDUPE_KEY_PREFIX + message.getShortSnapshotId.toString
        logger.info("couchbaseDedupe {}",key)
        if (!bucket.exists(key)) {
          logger.info("couchbaseDedupe contains the key{}",key)
          writeMessage(writer, message)
        }
        CorpCouchbaseClient_v2.returnClient(cacheClient)
      } catch {
        case e: Exception =>
          logger.error("Couchbase exception. Skip couchbase dedupe for this batch", e)
          metrics.meterByGauge("CBDedupeException",1)
          writeMessage(writer, message)
          couchbaseDedupe = false
      }
    } else {
      writeMessage(writer, message)
    }
  }

  /**
    * Write snapshort id as key to couchbase
 *
    * @param metaFiles meta files
    */
  private def writeSnapshort2Cb(metaFiles: MetaFiles) = {
    // insert new keys to couchbase for next round's dedupe
    if (metaFiles != null && couchbaseDedupe && metaFiles.metaFiles != null && metaFiles.metaFiles.length > 0) {
      val datesFiles = metaFiles.metaFiles
      if (datesFiles != null && datesFiles.length > 0) {
        datesFiles.foreach(dateFiles => {
          val df = this.readFilesAsDFEx(dateFiles.files)
          df.filter($"channel_action" === "CLICK" || $"channel_action" === "ROI")
            .repartition(30)
            .select("short_snapshot_id")
            .foreachPartition(partition => {
              // each partition insert cb by batch, limit the size to send to 5000 records
              partition.grouped(5000).foreach(
                group => {
                  var shortSnapshotIdList = new ListBuffer[String]()
                  try {
                    val (cacheClient, bucket) = CorpCouchbaseClient_v2.getBucketFunc()
                    group.foreach(record => shortSnapshotIdList += record.get(0).toString)
                    // async call couchbase batch api
                    Observable.from(shortSnapshotIdList.toArray).flatMap(new Func1[String, Observable[JsonDocument]]() {
                      override def call(shortSnapshotId: String): Observable[JsonDocument] = {
                        val shortSnapshotIdKey = DEDUPE_KEY_PREFIX + shortSnapshotId
                        bucket.async.upsert(JsonDocument.create(shortSnapshotIdKey, couchbaseTTL, JsonObject.empty()))
                      }
                    }).last().toBlocking.single()
                  } catch {
                    case e: Exception =>
                      logger.error("Couchbase exception. Skip couchbase dedupe for this batch", e)
                      metrics.meterByGauge("CBDedupeException",1)
                      couchbaseDedupe = false
                  }
                }
              )
            })
        })
      }
    }
  }

  /**
    * Output message to files
    * @param message
    */
  def writePartitionLag(message: FilterMessage) = {
    // write message lag to file with first message in this partition
    val timestampOfPartition = message.getTimestamp
    try {
      // write lag to hdfs file
      val output = fs.create(new Path(lagDir + TaskContext.get.partitionId()), true)
      output.writeBytes(String.valueOf(timestampOfPartition))
      output.writeBytes(System.getProperty("line.separator"))
      output.close()
    } catch {
      case e: Exception =>
        logger.error("Exception when writing message lag to file", e)
    }
  }

  /**
    * Flush the metrics
    */
  private def flushMetric = {
    if (metrics != null)
      metrics.flush()
  }

  /**
    * Write metrics of message
    * @param message
    */
  private def metric(message: FilterMessage) = {
    if (metrics != null) {
      metrics.meterByGauge("DedupeInputCount", 1,
        Field.of[String, AnyRef](CHANNEL_ACTION, message.getChannelAction.toString),
        Field.of[String, AnyRef](CHANNEL_TYPE, message.getChannelType.toString))
    }
  }

  /**
    * Dedupe in meta and compare last meta
    */
  def dedupeThisAndLastMeta(date: String): DateFiles = {
    // dedupe current df
    var df = readFilesAsDF(baseDir + "/" + date)

    df = df.dropDuplicates(SHORT_SNAPSHOT_ID_COL)
    val dedupeCompMeta = metadata.readDedupeCompMeta
    if (dedupeCompMeta != null && dedupeCompMeta.contains(date)) {
      val input = dedupeCompMeta.get(date).get
      val dfDedupe = readFilesAsDFEx(input)
        .select($"short_snapshot_id")
        .withColumnRenamed(SHORT_SNAPSHOT_ID_COL, "short_snapshot_id_1")

      df = df.join(dfDedupe, $"short_snapshot_id" === $"short_snapshot_id_1", "left_outer")
        .filter($"short_snapshot_id_1".isNull)
        .drop("short_snapshot_id_1")
    }

    // reduce the number of file
    df = df.repartition(params.partitions)

    saveDFToFiles(df, sparkDir)

    val files = renameFiles(outputDir, sparkDir, date)

    new DateFiles(date, files)
  }

  def setProperties(props: Properties) = {
    this.properties = props
  }

  def writeMessage(writer: ParquetWriter[GenericRecord], message: FilterMessage)  = {
    writer.write(message)
    if (metrics != null) {
      metrics.meterByGauge("DedupeTempOutput", 1,
        Field.of[String, AnyRef](CHANNEL_ACTION, message.getChannelAction.toString),
        Field.of[String, AnyRef](CHANNEL_TYPE, message.getChannelType.toString))
    }
  }
}
