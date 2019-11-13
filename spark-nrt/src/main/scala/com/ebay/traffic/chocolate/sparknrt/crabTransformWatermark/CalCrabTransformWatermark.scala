package com.ebay.traffic.chocolate.sparknrt.crabTransformWatermark

import java.io.ByteArrayOutputStream
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}

/**
 * Calculate dedupe and sink watermark and imk output watermark.
 * 1. crabTransform watermark: the minimum event_ts of current crabTransform output
 * 2. imkCrabTransform watermark: the minimum event_ts of current imkCrabTransformMerge output
 * 2. dedupe and sink watermark: the minimum lag of all kafka partitions
 * @author Zhiyuan Wang
 * @since 2019-09-03
 */
object CalCrabTransformWatermark extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new CalCrabTransformWatermark(params)

    job.run()
    job.stop()
  }
}

class CalCrabTransformWatermark(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode){

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore  _)

  @transient lazy val lvsFs: FileSystem = {
    val fs = FileSystem.get(URI.create(dedupeAndSinkKafkaLagDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient override lazy val fs: FileSystem = {
    val fs = FileSystem.get(URI.create(outputDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init("watermark-metrics-", params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else {
      null
    }
  }

  @transient lazy val schema_apollo = TableSchema("df_imk_apollo.json")

  // imk crab transform data dir
  lazy val imkCrabTransformDataDir: String = params.imkCrabTransformDataDir
  // crab transform data dir
  lazy val crabTransformDataDir: String = params.crabTransformDataDir
  // dedupe and sink kafka lag dir
  lazy val dedupeAndSinkKafkaLagDir: String = params.dedupAndSinkKafkaLagDir

  lazy val outputDir: String = params.outputDir

  override def run(): Unit = {
    val imkCrabTransformWatermark = getCrabTransformWatermark(imkCrabTransformDataDir)
    if (imkCrabTransformWatermark != null) {
      write(outputDir + "/imkCrabTransformWatermark", imkCrabTransformWatermark.toInstant.toEpochMilli.toString)
    }
    val crabTransformWatermark = getCrabTransformWatermark(crabTransformDataDir)
    if (crabTransformWatermark != null) {
      write(outputDir + "/crabTransformWatermark", crabTransformWatermark.toInstant.toEpochMilli.toString)
    }
    val kafkaWatermark = getKafkaWatermark
    kafkaWatermark.foreach(channelWatermarkTuple => {
      write(outputDir + "/dedupAndSinkWatermark" + "_" + channelWatermarkTuple._1, channelWatermarkTuple._2.toInstant.toEpochMilli.toString)
    })
    metrics.flush()
    metrics.close()
  }

  def write(path: String, outputValue: String) {
    val output = fs.create(new Path(path), true)
    output.writeBytes(outputValue)
    output.writeBytes(System.getProperty("line.separator"))
    output.close()
  }

  def readFileContent(path: Path, lvsFs: FileSystem): String = {
    val in = lvsFs.open(path)
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)
    var n = 0
    while(n > -1) {
      n = in.read(buffer)
      if(n > 0) {
        out.write(buffer, 0, n)
      }
    }
    in.close()
    out.toString.trim
  }

  def getCrabTransformWatermark(inputDir: String): ZonedDateTime = {
    val status = fs.listStatus(new Path(inputDir))
    val strings: Array[String] = status.map(s => s.getPath.toString)
    if (strings.isEmpty) {
      return null
    }
    val frame = readFilesAsDFEx(strings, schema_apollo.dfSchema, "sequence", "delete")
    val smallJoinDf = frame.select("event_ts")
    val watermark =  smallJoinDf.agg(min(smallJoinDf.col("event_ts"))).head().getString(0)
    ZonedDateTime.parse(watermark, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault()))
  }

  /**
   * Ignore invalid row
   *
   * @param values string array of row fields
   * @param schema dataframe schema
   * @return dataframe row
   */
  override def toDfRow(values: Array[String], schema: StructType): Row = {
    val validateSchema = (values.length == schema.fields.length) || (values.length == schema.fields.length + 1)
    if (!validateSchema) {
      metrics.meter("invalidValueLength")
      return null
    }
    try {
      Row(values zip schema map (e => {
        if (e._1.length == 0) {
          null
        } else {
          e._2.dataType match {
            case _: StringType => e._1.trim
            case _: LongType => e._1.trim.toLong
            case _: IntegerType => e._1.trim.toInt
            case _: ShortType => e._1.trim.toShort
            case _: FloatType => e._1.trim.toFloat
            case _: DoubleType => e._1.trim.toDouble
            case _: ByteType => e._1.trim.toByte
            case _: BooleanType => e._1.trim.toBoolean
          }
        }
      }): _*)
    } catch {
      case ex: Exception => {
        corruptRows.set(corruptRows.get + 1)
        if (corruptRows.get() <= MAX_CORRUPT_ROWS) {
          logger.warn("Failed to parse row: " + values.mkString("|"), ex)
          null
        } else {
          logger.error("Two many corrupt rows.")
          throw ex
        }
      }
    }
  }

  def getKafkaWatermark: Array[(String, ZonedDateTime)] = {
    params.channels.split(",").map(channel => {
      (channel, lvsFs.listStatus(new Path(dedupeAndSinkKafkaLagDir + "/" + channel))
        .map(status => status.getPath)
        .map(path => readFileContent(path, lvsFs).toLong)
        .map(ts => ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault()))
        .min(dateTimeOrdering))
    })
  }
}
