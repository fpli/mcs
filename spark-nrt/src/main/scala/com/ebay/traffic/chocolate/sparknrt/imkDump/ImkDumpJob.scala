package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.{InetAddress, URL}
import java.util
import java.util.Properties

import com.couchbase.client.java.document.JsonDocument
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import com.google.gson.{Gson, GsonBuilder}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import rx.Observable
import rx.functions.Func1

/**
  * Created by ganghuang on 12/3/18.
  * read capping result and generate files for imk table
  */
object ImkDumpJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDumpJob(params)

    job.run()
    job.stop()
  }
}

class ImkDumpJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode){
  lazy val outputDir: String = params.outPutDir + "/" + params.channel + "/imkDump/"

  lazy val sparkDir: String = params.workDir + "/imkDump/" + params.channel + "/spark/"

  lazy val METRICS_INDEX_PREFIX = "imkdump-metrics-"

  @transient var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("imk_dump.properties"))
    properties
  }

  @transient lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.ps"))
    Metadata(params.workDir, params.channel, usage)
  }

  @transient lazy val outputMetadata: Metadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.imkDump)
  }

  @transient lazy val schema_imk_table = TableSchema("df_imk.json")

  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  var guidCguidMap: util.HashMap[String, String] = {
    CorpCouchbaseClient.dataSource = properties.getProperty("imkdump.couchbase.datasource")
    null
  }
  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  override def run(): Unit = {
    // max number of metafiles at one job
    val batchSize: Int = {
      val batchSize = properties.getProperty("imkdump.metafile.batchsize")
      if (StringUtils.isNumeric(batchSize)) {
        Integer.parseInt(batchSize)
      } else {
        10 // default to 10 metafiles
      }
    }

    // clean temp folder
    fs.delete(new Path(sparkDir), true)
    fs.mkdirs(new Path(sparkDir))

    // metafiles for output data
    val suffix = properties.getProperty("imkdump.meta.output.suffix")
    var suffixArray: Array[String] = Array()
    if (StringUtils.isNotEmpty(suffix)) {
      suffixArray = suffix.split(",")
    }

    var dedupeOutputMeta = inputMetadata.readDedupeOutputMeta(".epnnrt")
    if (dedupeOutputMeta.length > batchSize) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, batchSize)
    }

    dedupeOutputMeta.foreach(metaIter => {
      val metaFile = metaIter._1
      val dataFiles = metaIter._2
      val outputMetas = dataFiles.map(dataFile => {
        val date = dataFile._1
        val df = readFilesAsDFEx(dataFile._2)
        logger.info("load DataFrame, " + date + ", with files=" + dataFile._2.mkString(","))

        val guidList = df
          .filter(needQueryCBToGetCguidUdf(col("cguid"), col("guid")))
          .select("guid")
          .distinct
          .collect()
          .map(row => row.get(0).toString)
        if (!guidList.isEmpty) {
          guidCguidMap = batchGetCguids(guidList)
        }

        val imkDf = imkDumpCore(df).repartition(params.partitions)

        metrics.meter("imk.dump.out", imkDf.count())

        saveDFToFiles(imkDf, sparkDir, "gzip", "csv", "bel")
        val files = renameFiles(outputDir, sparkDir, date)
        DateFiles(date, files)
      }).toArray

      outputMetadata.writeDedupeOutputMeta(MetaFiles(outputMetas), suffixArray)
      inputMetadata.deleteDedupeOutputMeta(metaFile)

    })

    if (metrics != null) {
      metrics.flush()
      metrics.close()
    }
  }

  def imkDumpCore(df: DataFrame): DataFrame = {
    var imkDf = df
      .withColumn("temp_uri_query", getQueryParamsUdf(col("uri")))
      .withColumn("batch_id", getBatchIdUdf())
      .withColumn("rvr_id", col("short_snapshot_id"))
      .withColumn("event_dt", getDateUdf(col("timestamp")))
      .withColumn("rvr_cmnd_type_cd", getCmndTypeUdf(col("channel_action")))
      .withColumn("rvr_chnl_type_cd", getChnanlTypeUdf(col("channel_type")))
      .withColumn("clnt_remote_ip", col("remote_ip"))
      .withColumn("brwsr_type_id", getBrowserTypeUdf(col("user_agent")))
      .withColumn("brwsr_name", col("user_agent"))
      .withColumn("rfrr_dmn_name", getLandingPageDomainUdf(col("referer")))
      .withColumn("rfrr_url", col("referer"))
      .withColumn("src_rotation_id", col("src_rotation_id"))
      .withColumn("dst_rotation_id", col("dst_rotation_id"))
      .withColumn("dst_client_id", getClientIdUdf(col("temp_uri_query"), lit("mkrid")))
      .withColumn("lndng_page_dmn_name", getLandingPageDomainUdf(col("uri")))
      .withColumn("lndng_page_url", col("uri"))
      .withColumn("user_query", getUserQueryUdf(col("referer"), col("temp_uri_query")))
      .withColumn("event_ts", getDateTimeUdf(col("timestamp")))
      .withColumn("perf_track_name_value", getPerfTrackNameValueUdf(col("temp_uri_query")))
      .withColumn("keyword", getKeywordUdf(col("temp_uri_query")))
      .withColumn("mt_id", getDefaultNullNumParamValueFromUrlUdf(col("temp_uri_query"), lit("mt_id")))
      .withColumn("crlp", getParamFromQueryUdf(col("temp_uri_query"), lit("crlp")))
      .withColumn("user_map_ind", getUserMapIndUdf(col("user_id")))
      .withColumn("item_id", getItemIdUdf(col("uri")))
      .withColumn("rvr_url", col("uri"))
      .withColumn("mfe_name", getParamFromQueryUdf(col("temp_uri_query"), lit("crlp")))
      .withColumn("cguid", getCguidUdf(col("cguid"), col("guid")))
      .drop("lang_cd")
      .filter(judegNotEbaySitesUdf(col("referer")))

    for (i <- 1 to 20) {
      val columnName = "flex_field_" + i
      val paramName = "ff" + i
      imkDf = imkDf.withColumn(columnName, getParamFromQueryUdf(col("temp_uri_query"), lit(paramName)))
    }

    schema_imk_table.filterNotColumns(imkDf.columns).foreach(e => {
      imkDf = imkDf.withColumn(e, lit(schema_imk_table.defaultValues(e)))
    })
    imkDf.select(schema_imk_table.dfColumns: _*)
  }

  val tools: Tools = new Tools(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
  val getQueryParamsUdf: UserDefinedFunction = udf((uri: String) => tools.getQueryString(uri))
  val getBatchIdUdf: UserDefinedFunction = udf(() => tools.getBatchId)
  val getDateUdf: UserDefinedFunction = udf((timestamp: Long) => tools.getDateFromTimestamp(timestamp))
  val getCmndTypeUdf: UserDefinedFunction = udf((channelType: String) => tools.getCommandType(channelType))
  val getChnanlTypeUdf: UserDefinedFunction = udf((channelType: String) => tools.getChannelType(channelType))
  val getBrowserTypeUdf: UserDefinedFunction = udf((userAgent: String) => tools.getBrowserType(userAgent))
  val getLandingPageDomainUdf: UserDefinedFunction = udf((uri: String) => tools.getDomain(uri))
  val getClientIdUdf: UserDefinedFunction = udf((query: String, ridParamName: String) => tools.getClientIdFromRotationId(tools.getParamValueFromQuery(query, ridParamName)))
  val getUserQueryUdf: UserDefinedFunction = udf((referer: String, query: String) => tools.getUserQuery(referer, query))
  val getDateTimeUdf: UserDefinedFunction = udf((timestamp: Long) => tools.getDateTimeFromTimestamp(timestamp))
  val getPerfTrackNameValueUdf: UserDefinedFunction = udf((query: String) => tools.getPerfTrackNameValue(query))
  val getKeywordUdf: UserDefinedFunction = udf((query: String) => tools.getParamFromQuery(query, tools.keywordParams))
  val getDefaultNullNumParamValueFromUrlUdf: UserDefinedFunction = udf((query: String, key: String) => tools.getDefaultNullNumParamValueFromQuery(query, key))
  val getParamFromQueryUdf: UserDefinedFunction = udf((query: String, key: String) => tools.getParamValueFromQuery(query, key))
  val getUserMapIndUdf: UserDefinedFunction = udf((userId: String) => tools.getUserMapInd(userId))
  val getItemIdUdf: UserDefinedFunction = udf((uri: String) => tools.getItemIdFromUri(uri))
  val judegNotEbaySitesUdf: UserDefinedFunction = udf((referer: String) => tools.judgeNotEbaySites(referer))
  val needQueryCBToGetCguidUdf: UserDefinedFunction = udf((cguid: String, guid: String) => StringUtils.isEmpty(cguid) && StringUtils.isNotEmpty(guid))
  val getCguidUdf: UserDefinedFunction = udf((cguid: String, guid: String) => getCguid(cguid, guid))
  /**
    * override renameFiles to have special output file name for TD
    * @param outputDir final destination
    * @param workDir temp output
    * @param date current handled date
    * @return files array handled
    */
  override def renameFiles(outputDir: String, workDir: String, date: String): Array[String] = {
    // rename result to output dir
    val dateOutputPath = new Path(outputDir + "/" + date)
    if (!fs.exists(dateOutputPath)) {
      fs.mkdirs(dateOutputPath)
    }
    val hostName = InetAddress.getLocalHost.getHostName

    val fileStatus = fs.listStatus(new Path(workDir))
    val files = fileStatus
      .filter(status => status.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%4d" format swi._2).replace(" ", "0")
        //        imk_rvr_trckng_????????_??????.V4.*dat.gz
        val target = new Path(dateOutputPath, "imk_rvr_trckng_" + tools.getOutPutFileDate + ".V4." + hostName + "." + params.channel + seq + ".dat.gz")
        logger.info("Rename from: " + src.toString + " to: " + target.toString)
        fs.rename(src, target)
        target.toString
      })
    files
  }

  /**
    * get cguid
    * @param cguid cguid
    * @param guid guid
    * @return
    */
  def getCguid(cguid: String, guid: String): String = {
    if (StringUtils.isNotEmpty(cguid)) {
      cguid
    } else {
      metrics.meter("imk.dump.tryCguidByGuid", 1)
      if (guidCguidMap != null) {
        val result = guidCguidMap.getOrDefault(guid, "")
        if (StringUtils.isNotEmpty(result)) {
          metrics.meter("imk.dump.gotCguidByGuid", 1)
        }
        result
      } else {
        ""
      }
    }
  }

  /**
    * async get cguid by guid list
    * @param list guid list
    * @return
    */
  def batchGetCguids(list: Array[String]): util.HashMap[String, String] = {
    val res = new util.HashMap[String, String]
    val (cacheClient, bucket) = CorpCouchbaseClient.getBucketFunc()
    try {
      val jsonDocuments = Observable
        .from(list)
        .flatMap(new Func1[String, Observable[JsonDocument]]() {
          override def call(key: String): Observable[JsonDocument] = {
            bucket.async.get(key, classOf[JsonDocument])
          }
        }).toList.toBlocking.single
      val gson = new Gson()
      for(i <- 0 until jsonDocuments.size()) {
        val element = jsonDocuments.get(i)
        val cguidObject = gson.fromJson(String.valueOf(element.content()), classOf[Cguid])
        if (cguidObject != null) {
          res.put(element.id(), cguidObject.getCguid)
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase error while getting cguid by guid list" +  e)
        metrics.meter("imk.dump.error.cbquery", 1)
        // should we throw the exception and make the job fail?
        throw new Exception(e)
      }
    }
    CorpCouchbaseClient.returnClient(cacheClient)
    res
  }

}
