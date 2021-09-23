package com.ebay.traffic.chocolate.sparknrt.crabDedupe

import java.net.URI
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{ZoneId, ZonedDateTime}

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.ebay.kernel.util.StringUtils
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object CrabDedupeJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new CrabDedupeJob(params)
    job.run()
    job.stop()
  }
}

class CrabDedupeJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  import spark.implicits._

  lazy val dedupeOutTempDir: String = params.workDir + "/" + params.appName  + "/outTemp"
  lazy val outputDir: String = params.outputDir + "/" + params.appName
  lazy val DEDUPE_KEY_PREFIX: String = params.appName + "_"
  lazy val couchbaseTTL: Int = params.couchbaseTTL
  lazy val METRICS_INDEX_PREFIX: String = "crab-metrics-"
  var LAG_FILE: String = "hdfs://elvisha/apps/tracking-events-workdir/last_ts/crabDedupe/crab_min_ts"
  lazy val compression: String = {
    if(params.snappyCompression) {
      "snappy"
    } else {
      "uncompressed"
    }
  }
  lazy val dataFileSuffix: String = {
    if(params.snappyCompression) {
      "snappy"
    } else {
      "csv"
    }
  }

  var couchbaseDedupe: Boolean = {
    params.couchbaseDedupe
  }

  @transient lazy val lvsFs: FileSystem = {
    val fs = FileSystem.get(URI.create(params.inputDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient override lazy val fs: FileSystem = {
    val fs = FileSystem.get(URI.create(params.workDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val schema_tfs = TableSchema("df_imk.json")
  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }
  @transient lazy val outputMetadata: Metadata = {
    Metadata(params.workDir, params.appName, MetadataEnum.dedupe)
  }
  @transient lazy val inputFiles: Array[String] = {
    val files = getInputFiles(params.inputDir)
    if (files.length > params.maxDataFiles) {
      metrics.meter("crab.dedupe.TooManyFiles")
      files.slice(0, params.maxDataFiles)
    } else {
      files
    }
  }
  @transient lazy val couchbase: CorpCouchbaseClient.type = {
    metrics.meter("crab.dedupe.InitCB")
    CorpCouchbaseClient.dataSource = params.couchbaseDatasource
    CorpCouchbaseClient
  }

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  override def run(): Unit = {

    // clear temp folders, clear
    fs.delete(new Path(dedupeOutTempDir), true)

    logger.info("read files num %s".format(inputFiles.length))
    logger.info("read files %s".format(inputFiles.mkString(",")))

    val df = readFilesAsDFEx(inputFiles, schema_tfs.dfSchema, "csv", "bel")
      .dropDuplicates("rvr_id")
      .cache()

    val lag =  df.agg(min(df.col("event_ts"))).head().getString(0)

    val minEventDateTime = toDateTime(lag)
    if (minEventDateTime.isDefined) {
      val output = lvsFs.create(new Path(LAG_FILE), true)
      logger.info("min event_ts %s".format(lag))
      output.writeBytes(minEventDateTime.get.toInstant.toEpochMilli.toString)
      output.writeBytes(System.getProperty("line.separator"))
      output.close()
    }

    val dates = df.select("event_dt").distinct().collect().map(row => {
      val eventDt = row.get(0)
      if (eventDt == null) {
        metrics.meter("crab.dedupe.NullEventDt")
        null
      } else {
        row.get(0).toString
      }
    }).filter(date => date != null)
    val outputMetas = dates.map(date => {
      val oneDayDf = df
        .filter($"event_dt" === date)
        .filter(dedupeRvrIdByCBUdf(col("rvr_id"), col("rvr_cmnd_type_cd")))
        .repartition(params.partitions)
      val tempOutFolder = dedupeOutTempDir + "/date=" + date
      saveDFToFiles(oneDayDf, tempOutFolder)
      val files = renameFiles(outputDir, tempOutFolder, "date=" + date)
      logger.info("output %s %s".format(date, files.mkString(",")))
      DateFiles("date=" + date, files)
    })
    outputMetadata.writeDedupeOutputMeta(MetaFiles(outputMetas))
    inputFiles.foreach(file => {
      logger.info("delete %s".format(file))
      lvsFs.delete(new Path(file), true)
    })
    metrics.meter("crab.dedupe.processedFiles", inputFiles.length)
    if(metrics != null) {
      metrics.flush()
      metrics.close()
    }
  }

  /**
   * Convert datetime string to ZoneDateTime
   * @param dateTimeStr datetime string
   * @return ZoneDateTime or None if the dateTimeStr format is wrong
   */
  def toDateTime(dateTimeStr: String): Option[ZonedDateTime] = {
    if (StringUtils.isEmpty(dateTimeStr)) {
      logger.warn("event_ts is empty")
      metrics.meter("crab.dedupe.InvalidEventTs")
      return None
    }
    try{
      Some(ZonedDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault())))
    } catch {
      case _: DateTimeParseException => {
        logger.warn("invalid event_ts {}", dateTimeStr)
        metrics.meter("crab.dedupe.InvalidEventTs")
        None
      }
    }
  }

  val dedupeRvrIdByCBUdf: UserDefinedFunction = udf((rvrId: String, cmndType: String) => dedupeRvrIdByCB(rvrId, cmndType) )

  /**
    * use couchbase to dedupe rvr id
    * @param rvrId rvr id
    * @return
    */
  def dedupeRvrIdByCB(rvrId: String, cmndType: String): Boolean = {
    var result = true
    if (couchbaseDedupe && !cmndType.equals("4")) {
      try {
        val (cacheClient, bucket) = couchbase.getBucketFunc()
        val key = DEDUPE_KEY_PREFIX + rvrId
        if (!bucket.exists(key)) {
          bucket.upsert(JsonDocument.create(key, couchbaseTTL, JsonObject.empty()))
        } else {
          result = false
        }
        couchbase.returnClient(cacheClient)
      } catch {
        case e: Exception =>
          logger.error("Couchbase exception. Skip couchbase dedupe for this batch", e)
          metrics.meter("crab.dedupe.CBError")
//          couchbaseDedupe = false
      }
    }
    result
  }

  /**
    * get crab job upload data files
    * @param inputDir carb upload data files location
    * @return
    */
  def getInputFiles(inputDir: String): Array[String] = {
    val status = lvsFs.listStatus(new Path(inputDir))
    status.filter(path => path.getPath.getName.startsWith("imk_rvr_trckng"))
      .map(path => path.getPath.toString)
  }
}
