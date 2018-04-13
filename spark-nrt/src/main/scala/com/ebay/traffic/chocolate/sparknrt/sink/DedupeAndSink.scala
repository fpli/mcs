package com.ebay.traffic.chocolate.sparknrt.sink

import java.security.SecureRandom
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.ebay.app.raptor.chocolate.avro.FilterMessage
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.spark.kafka.KafkaRDD
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yliu29 on 3/8/18.
  */
object DedupeAndSink extends App {
  override def main(args: Array[String]) = {
    val params = Parameter(args)

    val job = new DedupeAndSink(params)

    job.run()
    job.stop()
  }
}

class DedupeAndSink(params: Parameter)
  extends BaseSparkJob(params.appName, params.mode) {

  @transient var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("kafka.properties"))
    properties
  }

  lazy val DATE_COL = "date"

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def getDateString(timestamp: Long): String = {
    sdf.format(new Date(timestamp))
  }

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val rand = {
    new SecureRandom()
  }

  lazy val baseDir = params.workDir + "/dedupe/" + params.channel + "/"
  lazy val baseTempDir = baseDir + "/tmp/"
  lazy val sparkDir = baseDir + "/spark/"
  lazy val outputDir = params.outputDir

  @transient lazy val metadata = {
    Metadata(params.workDir, params.channel)
  }

  val SNAPSHOT_ID_COL = "snapshot_id"

  import spark.implicits._

  override def run() = {

    logger.info("baseDir: " + baseDir)
    logger.info("outputDir: " + outputDir)

    // clean base dir
    fs.delete(new Path(baseDir), true)
    fs.mkdirs(new Path(baseDir))
    fs.mkdirs(new Path(baseTempDir))
    fs.mkdirs(new Path(sparkDir))

    val kafkaRDD = new KafkaRDD[java.lang.Long, FilterMessage](
      sc, params.kafkaTopic, properties, params.maxConsumeSize)

    val dates =
    kafkaRDD.mapPartitions(iter => {
      val files = new util.HashMap[String, String]()
      val writers = new util.HashMap[String, ParquetWriter[GenericRecord]]()

      // output messages to files
      while (iter.hasNext) {
        val message = iter.next().value()
        val date = DATE_COL + "=" + getDateString(message.getTimestamp) // get the event date
        var writer = writers.get(date)
        if (writer == null) {
          val file = "/" + date + "/" + Math.abs(rand.nextLong()) + ".parquet"
          files.put(date, file)

          logger.info("Create AvroParquetWriter for path: " + baseTempDir + file)

          writer = AvroParquetWriter.
            builder[GenericRecord](new Path(baseTempDir + file))
            .withSchema(FilterMessageV1.getClassSchema())
            .withConf(hadoopConf)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()

          writers.put(date, writer)
        }
        // write message
        writer.write(message)
      }

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
    val metaFiles = new MetaFiles(dates.map(date => dedupe(date)))

    metadata.writeDedupeCompMeta(metaFiles)
    metadata.writeDedupeOutputMeta(metaFiles)

    // commit offsets of kafka RDDs
    kafkaRDD.commitOffsets()
    kafkaRDD.close()

    // delete the dir
    fs.delete(new Path(baseDir), true)
  }

  /**
    * Dedupe logic
    */
  def dedupe(date: String): DateFiles = {
    // dedupe current df
    var df = readFilesAsDF(baseDir + "/" + date).dropDuplicates(SNAPSHOT_ID_COL)
    val dedupeCompMeta = metadata.readDedupeCompMeta
    if (dedupeCompMeta != null && dedupeCompMeta.contains(date)) {
      val input = dedupeCompMeta.get(date).get
      val dfDedupe = readFilesAsDFEx(input)
        .select($"snapshot_id")
        .withColumnRenamed(SNAPSHOT_ID_COL, "snapshot_id_1")

      df = df.join(dfDedupe, $"snapshot_id" === $"snapshot_id_1", "left_outer")
        .filter($"snapshot_id_1".isNull)
        .drop("snapshot_id_1")
    }

    // reduce the number of file
    df = df.repartition(params.partitions)

    saveDFToFiles(df, sparkDir)

    val dateOutputPath = new Path(outputDir + "/" + date)
    var max = -1
    if (fs.exists(dateOutputPath)) {
      val outputStatus = fs.listStatus(dateOutputPath)
      if (outputStatus.length > 0) {
        max = outputStatus.map(status => {
          val name = status.getPath.getName
          Integer.valueOf(name.substring(5, name.indexOf(".")))
        }).sortBy(i => i).last
      }
    } else {
      fs.mkdirs(dateOutputPath)
    }

    val fileStatus = fs.listStatus(new Path(sparkDir))
    val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format max + 1 + swi._2).replace(" ", "0")
        val target = new Path(dateOutputPath, s"part-${seq}.snappy.parquet")
        logger.info("Rename from: " + src.toString + " to: " + target.toString)
        fs.rename(src, target)
        target.toString
      })

    new DateFiles(date, files)
  }

  def setProperties(props: Properties) = {
    this.properties = props
  }
}
