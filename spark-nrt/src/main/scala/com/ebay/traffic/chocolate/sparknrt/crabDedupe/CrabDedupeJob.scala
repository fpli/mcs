package com.ebay.traffic.chocolate.sparknrt.crabDedupe

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.imkDump.TableSchema
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import org.apache.hadoop.fs.Path
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

    val df = readFilesAsDFEx(inputFiles, schema_tfs.dfSchema, "csv", "bel")
      .dropDuplicates("rvr_id")
      .cache()
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
      saveDFToFiles(oneDayDf, tempOutFolder, compression, "csv", "bel")
      val files = renameFiles(outputDir, tempOutFolder, "date=" + date)
      DateFiles("date=" + date, files)
    })
    outputMetadata.writeDedupeOutputMeta(MetaFiles(outputMetas))
    inputFiles.foreach(file => {
      fs.delete(new Path(file), true)
    })
    metrics.meter("crab.dedupe.processedFiles", inputFiles.length)
    if(metrics != null) {
      metrics.flush()
      metrics.close()
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
          couchbaseDedupe = false
      }
    }
    result
  }

  /**
    * move to destination folder
    * @param outputDir final destination
    * @param sparkDir temp result dir
    * @param date current handled date
    * @return files array handled
    */
  override def renameFiles(outputDir: String, sparkDir: String, date: String): Array[String] = {
    // rename result to output dir
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
        val target = new Path(dateOutputPath, s"part-$seq.$dataFileSuffix")
        logger.info("Rename from: " + src.toString + " to: " + target.toString)
        fs.rename(src, target)
        target.toString
      })
    files
  }

  /**
    * get crab job upload data files
    * @param inputDir carb upload data files location
    * @return
    */
  def getInputFiles(inputDir: String): Array[String] = {
    val status = fs.listStatus(new Path(inputDir))
    status.filter(path => path.getPath.getName.startsWith("imk_rvr_trckng"))
      .map(path => path.getPath.toString)
  }
}
