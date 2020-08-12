package com.ebay.traffic.chocolate.sparknrt.imkETL

import java.net.{InetAddress, URI, URLDecoder}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.couchbase.client.java.document.JsonDocument
import com.ebay.kernel.patternmatch.dawg.{Dawg, DawgDictionary}
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.imkDump.Tools
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.chocolate.sparknrt.utils.{Cguid, MyID, TableSchema, XIDResponse}
import com.ebay.traffic.monitoring.{ESMetrics, Field, Metrics}
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import scalaj.http.Http
import spray.json._
import rx.Observable
import rx.functions.Func1

import scala.collection.mutable
import scala.io.Source

/**
 * ETL pipeline for all channels.
 *
 * @author Zhiyuan Wang
 * @since 2019/12/18
 */
object ImkETLJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkETLJob(params)

    job.run()
    job.stop()
  }
}

class ImkETLJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {
  // imk dump job output, for uc4 etl job
  lazy val imkDumpOutputDir: String = params.outPutDir

  lazy val imkDumpTempDir: String = params.outPutDir + "/imkETL/dumpTemp/"

  // imk crabTransform output，for apollo job, the dir same with imkTransform job
  lazy val imkETLOutputDir: String = params.outPutDir + "/imkTransform/imkOutput/"
  lazy val dtlETLOutputDir: String = params.outPutDir + "/imkTransform/dtlOutput/"
  lazy val mgETLOutputDir: String = params.outPutDir + "/imkTransform/mgOutput/"

  lazy val imkETLTempDir: String = params.outPutDir + "/imkETL/imkTemp/"
  lazy val dtlETLTempDir: String = params.outPutDir + "/imkETL/dtlTemp/"
  lazy val mgETLTempDir: String = params.outPutDir + "/imkETL/mgTemp/"

  lazy val METRICS_INDEX_PREFIX = "imk-etl-metrics-"

  @transient lazy val workDirFs = {
    val fs = FileSystem.get(URI.create(params.workDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("imk_etl.properties"))
    properties
  }

  @transient lazy val inputMetadataFiles: Array[(String, Metadata)] = {
    params.channel.split(",").map({
      case "PAID_SEARCH" =>
        "PAID_SEARCH" -> Metadata(params.workDir, "PAID_SEARCH", MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.ps")))
      case "DISPLAY" =>
        "DISPLAY" -> Metadata(params.workDir, "DISPLAY", MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.display")))
      case "ROI" =>
        "ROI" -> Metadata(params.workDir, "ROI", MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.roi")))
      case "SOCIAL_MEDIA" =>
        "SOCIAL_MEDIA" -> Metadata(params.workDir, "SOCIAL_MEDIA", MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.social")))
      case "SEARCH_ENGINE_FREE_LISTINGS" =>
        "SEARCH_ENGINE_FREE_LISTINGS" -> Metadata(params.workDir, "SEARCH_ENGINE_FREE_LISTINGS", MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.search-engine-free-listings")))
    })
  }

  @transient lazy val schema_imk_table: TableSchema = TableSchema("df_imk.json")

  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  // by default, no suffix
  @transient lazy val CHANNEL_META_POSTFIX_MAP = Map(
    "PAID_SEARCH" -> "",
    "DISPLAY" -> "",
    "ROI" -> "",
    "SOCIAL_MEDIA" -> "",
    "SEARCH_ENGINE_FREE_LISTINGS" -> ""
  )

  var guidCguidMap: util.HashMap[String, String] = {
    CorpCouchbaseClient.dataSource = properties.getProperty("imkdump.couchbase.datasource")
    null
  }

  @transient lazy val userAgentBotDawgDictionary: DawgDictionary = {
    val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("dap_user_agent_robot.txt")).getLines.toArray
    new DawgDictionary(lines, true)
  }

  @transient lazy val ipBotDawgDictionary: DawgDictionary = {
    val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("dap_ip_robot.txt")).getLines.toArray
    new DawgDictionary(lines, true)
  }

  @transient lazy val schema_apollo: TableSchema = TableSchema("df_imk_apollo.json")
  @transient lazy val schema_apollo_dtl: TableSchema = TableSchema("df_imk_apollo_dtl.json")
  @transient lazy val schema_apollo_mg: TableSchema = TableSchema("df_imk_apollo_mg.json")

  @transient lazy val mfe_name_id_map: Map[String, String] = {
    val mapData = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("mfe_name_id_map.txt")).getLines
    mapData.map(line => line.split("\\|")(0) -> line.split("\\|")(1)).toMap
  }

  lazy val xidHost: String = properties.getProperty("xid.xidHost")
  lazy val xidConsumerId: String = properties.getProperty("xid.xidConsumerId")
  lazy val xidClientId: String = properties.getProperty("xid.xidClientId")
  lazy val xidConnectTimeout: Int = properties.getProperty("xid.xidConnectTimeout").toInt
  lazy val xidReadTimeout: Int = properties.getProperty("xid.xidReadTimeout").toInt

  @transient lazy val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  @transient lazy val compressCodec: Option[Class[GzipCodec]] = {
    if (params.compressOutPut) {
      Some(classOf[GzipCodec])
    } else {
      None
    }
  }

  import spark.implicits._

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to run the spark job.
   */
  override def run(): Unit = {
    // max number of single channel meta files at one job
    val singleChannelBatchSize: Int = {
      val size = properties.getProperty("imkdump.metafile.singlechannel.batchsize")
      if (StringUtils.isNumeric(size)) {
        Integer.parseInt(size)
      } else {
        // default to 5 metafiles
        5
      }
    }

    // input metas for all channels
    val inputMeta = inputMetadataFiles.map(kv => {
      val channel = kv._1
      var singleChannelMeta = kv._2.readDedupeOutputMeta(CHANNEL_META_POSTFIX_MAP(channel))
      if (singleChannelMeta.length > singleChannelBatchSize) {
        singleChannelMeta = singleChannelMeta.slice(0, singleChannelBatchSize)
      }
      (channel, singleChannelMeta)
    })

    // imk dump for all channels, same with ImkDumpJob
    val imkDumpDfMap = imkDumpJob(inputMeta)

    // imk crab transform for all channels, same with CrabTransformJob
    imkTransformJob(imkDumpDfMap)

    // delete input metas
    inputMeta.foreach(kv => {
      val channel = kv._1
      kv._2.foreach(metaIter => {
        val metaFile = metaIter._1
        logger.info("delete meta %s".format(metaFile))
        // metaFile is located in workDir
        workDirFs.delete(new Path(metaFile), true)
      })
      metrics.meter("imk.transform.processedMete", kv._2.length, Field.of[String, AnyRef]("channelType", channel))
    })

    if (metrics != null) {
      metrics.flush()
    }
    if (tools.metrics != null) {
      tools.metrics.flush()
    }
  }

  /**
   * imk dump for all channels, same with ImkDumpJob
   * @param inputMeta input meta
   * @return imk dump dataframe
   */
  def imkDumpJob(inputMeta: Array[(String, Array[(String, Map[String, Array[String]])])]): mutable.Map[String, mutable.Map[String, DataFrame]] = {
    // metafiles for imk dump output data
    val suffix = properties.getProperty("imkdump.meta.output.suffix")
    var suffixArray: Array[String] = Array()
    if (StringUtils.isNotEmpty(suffix)) {
      suffixArray = suffix.split(",")
    }

    // clean temp folder
    fs.delete(new Path(imkDumpTempDir), true)
    fs.mkdirs(new Path(imkDumpTempDir))

    val imkDumpDfMap = mutable.Map[String, mutable.Map[String, DataFrame]]()

    inputMeta.foreach(kv => {
      val channel = kv._1
      val singleChannelImkDumpDf = mutable.Map[String, DataFrame]()
      kv._2.foreach(metaIter => {
        val outputMetas = metaIter._2.map(dateFile => {
          val date = dateFile._1
          logger.info("load DataFrame, %s, with files=%s".format(date, dateFile._2.mkString(",")))
          val df = readFilesAsDFEx(dateFile._2)

          val guidList = df
            .filter(needQueryCBToGetCguidUdf(col("cguid"), col("guid")))
            .select("guid")
            .distinct
            .collect()
            .map(row => row.get(0).toString)
          if (!guidList.isEmpty) {
            guidCguidMap = batchGetCguids(channel, guidList)
          }
          val imkDumpDf = imkDump(df)

          import org.apache.spark.sql.catalyst.encoders.RowEncoder
          implicit val encoder: ExpressionEncoder[Row] = RowEncoder(imkDumpDf.schema)
          val imkDumpRepartitionDf = imkDumpDf.mapPartitions((iter: Iterator[Row]) => {
            if (metrics != null) {
              metrics.flush()
            }
            if (tools.metrics != null) {
              tools.metrics.flush()
            }
            iter
          }).cache()

          // save to hdfs, for UC4 etl job
          saveDFToFiles(imkDumpRepartitionDf, imkDumpTempDir, "gzip", "csv", "bel")

          val files = renameFiles(imkDumpOutputDir, imkDumpTempDir, date, channel)

          if (singleChannelImkDumpDf.contains(date)) {
            singleChannelImkDumpDf.put(date, singleChannelImkDumpDf(date).union(imkDumpRepartitionDf))
          } else {
            singleChannelImkDumpDf.put(date, imkDumpRepartitionDf)
          }

          DateFiles(date, files)
        }).toArray
        val outputMetadata = Metadata(params.workDir, channel, MetadataEnum.imkDump)
        outputMetadata.writeDedupeOutputMeta(MetaFiles(outputMetas), suffixArray)
      })
      imkDumpDfMap.put(channel, singleChannelImkDumpDf)
    })
    imkDumpDfMap
  }

  /**
   * parse common fields
   * @param df input df
   * @return df with appended fields
   */
  def imkDumpCommon(df: DataFrame): DataFrame = {
    df
      .withColumn("temp_uri_query", getQueryParamsUdf(col("uri")))
      .withColumn("batch_id", getBatchIdUdf())
      .withColumn("rvr_id", col("short_snapshot_id"))
      .withColumn("event_dt", getDateUdf(col("timestamp")))
      .withColumn("rvr_cmnd_type_cd", getCmndTypeUdf(col("channel_action")))
      .withColumn("rvr_chnl_type_cd", getChannelTypeUdf(col("channel_type")))
      .withColumn("clnt_remote_ip", col("remote_ip"))
      .withColumn("brwsr_type_id", getBrowserTypeUdf(col("user_agent")))
      .withColumn("brwsr_name", col("user_agent"))
      .withColumn("rfrr_dmn_name", getLandingPageDomainUdf(col("referer")))
      .withColumn("rfrr_url", col("referer"))
      .withColumn("src_rotation_id", col("src_rotation_id"))
      .withColumn("dst_rotation_id", col("dst_rotation_id"))
      .withColumn("lndng_page_dmn_name", getLandingPageDomainUdf(col("uri")))
      .withColumn("lndng_page_url", replaceMkgroupidMktypeUdfAndParseMpreFromRoverUdf(col("channel_type"), col("uri")))
      .withColumn("user_query", getUserQueryUdf(col("referer"), col("temp_uri_query")))
      .withColumn("event_ts", getDateTimeUdf(col("timestamp")))
      .withColumn("perf_track_name_value", getPerfTrackNameValueUdf(col("temp_uri_query")))
      .withColumn("keyword", getKeywordUdf(col("temp_uri_query")))
      .withColumn("mt_id", getDefaultNullNumParamValueFromUrlUdf(col("temp_uri_query"), lit("mt_id")))
      .withColumn("crlp", getParamFromQueryUdf(col("temp_uri_query"), lit("crlp")))
      .withColumn("user_map_ind", getUserMapIndUdf(col("user_id")))
      .withColumn("rvr_url", replaceMkgroupidMktypeUdf(col("channel_type"), col("uri")))
      .withColumn("mfe_name", getParamFromQueryUdf(col("temp_uri_query"), lit("crlp")))
      .withColumn("cguid", getCguidUdf(col("channel_type"), col("cguid"), col("guid")))
  }

  /**
   * parse fields base on channel type
   * @param df input df
   * @return df with appended fields
   */
  def imkDumpSpecific(df: DataFrame): DataFrame = {
    df
      .withColumn("dst_client_id", getClientIdUdf(col("channel_type"), col("temp_uri_query"), lit("mkrid"), col("uri")))
      .withColumn("item_id", getItemIdUdf(col("channel_type"), col("temp_uri_query"), col("uri")))
      .withColumn("roi_item_id", getRoiItemIdUdf(col("channel_type"), col("temp_uri_query"), col("uri")))
      .withColumn("transaction_id", getTransactionIdUdf(col("channel_type"), lit(2), col("temp_uri_query")))
      .withColumn("transaction_type", getTransactionTypeUdf(col("channel_type"), col("temp_uri_query"), lit("tranType")))
      .withColumn("cart_id", getCartIdUdf(col("channel_type"), lit(3), col("temp_uri_query")))
      .withColumn("ebay_site_id", getEbaySiteIdUdf(col("channel_type"), col("temp_uri_query"), lit("siteId")))
      .withColumn("mgvalue", getMgvalueUdf(col("channel_type")))
      .withColumn("mgvaluereason", getMgvaluereasonUdf(col("channel_type"), col("brwsr_name"), col("clnt_remote_ip")))
  }

  val judegNotEbaySitesUdf: UserDefinedFunction = udf((channelType: String, referer: String) => {
    channelType match {
      case "ROI" => true
      case _ => tools.judgeNotEbaySites(referer)
    }
  })

  val getClientIdUdf: UserDefinedFunction = udf((channelType: String, tempUriQuery: String, ridParamName: String, uri: String) => {
    channelType match {
      case "ROI" => tools.getClientIdFromRoverUrl(uri)
      case _ => tools.getClientIdFromRotationId(tools.getParamValueFromQuery(tempUriQuery, ridParamName))
    }
  })

  val getItemIdUdf: UserDefinedFunction = udf((channelType: String, tempUriQuery: String, uri: String) => {
    channelType match {
      case "ROI" => tools.getRoiIdFromUrlQuery(1, tempUriQuery)
      case _ => tools.getItemIdFromUri(uri)
    }
  })

  val getRoiItemIdUdf: UserDefinedFunction = udf((channelType: String, tempUriQuery: String, uri: String) => {
    channelType match {
      case "ROI" => tools.getRoiIdFromUrlQuery(1, tempUriQuery)
      case _ => ""
    }
  })

  val getTransactionIdUdf: UserDefinedFunction = udf((channelType: String, index: Int, tempUriQuery: String) => {
    channelType match {
      case "ROI" => tools.getRoiIdFromUrlQuery(index, tempUriQuery)
      case _ => ""
    }
  })

  val getTransactionTypeUdf: UserDefinedFunction = udf((channelType: String, tempUriQuery: String, tranType: String) => {
    channelType match {
      case "ROI" => tools.getParamValueFromQuery(tempUriQuery, tranType)
      case _ => ""
    }
  })

  val getCartIdUdf: UserDefinedFunction = udf((channelType: String, index: Int, tempUriQuery: String) => {
    channelType match {
      case "ROI" => tools.getRoiIdFromUrlQuery(index, tempUriQuery)
      case _ => ""
    }
  })

  val getEbaySiteIdUdf: UserDefinedFunction = udf((channelType: String, tempUriQuery: String, siteId: String) => {
    channelType match {
      case "ROI" => tools.getParamValueFromQuery(tempUriQuery, siteId)
      case _ => ""
    }
  })

  val getMgvalueUdf: UserDefinedFunction = udf((channelType: String) => {
    channelType match {
      case "ROI" => "0"
      case "DISPLAY" => "0"
      case _ => ""
    }
  })

  val getMgvaluereasonUdf: UserDefinedFunction = udf((channelType: String, brwsrName: String, clntRemoteIp: String) => {
    channelType match {
      case "ROI" => getMgvaluereason(brwsrName, clntRemoteIp)
      case "DISPLAY" => getMgvaluereason(brwsrName, clntRemoteIp)
      case _ => ""
    }
  })

  /**
   * Generate mgvaluereason, "4" or empty string, "4" means BOT
   *
   * @param brwsrName    alias for user agent
   * @param clntRemoteIp alias for remote ip
   * @return "4" or empty string
   */
  def getMgvaluereason(brwsrName: String, clntRemoteIp: String): String = {
    if (isBotByUserAgent(brwsrName) || isBotByIp(clntRemoteIp)) {
      "4"
    } else {
      ""
    }
  }

  /**
   * parse flex fields and filter output by schema
   * @param df input df
   * @return df with final schema
   */
  def imkDumpEx(df: DataFrame): DataFrame = {
    var imkDf = df
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

  def imkDump(df: DataFrame): DataFrame = {
    val commonDf = imkDumpCommon(df)
    val imkDf = imkDumpSpecific(commonDf)
      .drop("lang_cd")
      .filter(judegNotEbaySitesUdf(col("channel_type"), col("referer")))
    imkDumpEx(imkDf)
  }

  val tools: Tools = new Tools(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
  val getQueryParamsUdf: UserDefinedFunction = udf((uri: String) => tools.getQueryString(uri))
  val getBatchIdUdf: UserDefinedFunction = udf(() => tools.getBatchId)
  val getDateUdf: UserDefinedFunction = udf((timestamp: Long) => tools.getDateFromTimestamp(timestamp))
  val getCmndTypeUdf: UserDefinedFunction = udf((channelType: String) => tools.getCommandType(channelType))
  val getChannelTypeUdf: UserDefinedFunction = udf((channelType: String) => tools.getChannelType(channelType))
  val getBrowserTypeUdf: UserDefinedFunction = udf((userAgent: String) => tools.getBrowserType(userAgent))
  val getLandingPageDomainUdf: UserDefinedFunction = udf((uri: String) => tools.getDomain(uri))
  val getUserQueryUdf: UserDefinedFunction = udf((referer: String, query: String) => tools.getUserQuery(referer, query))
  val replaceMkgroupidMktypeUdf: UserDefinedFunction = udf((channelType: String, uri: String) => replaceMkgroupidMktype(channelType, uri))
  val replaceMkgroupidMktypeUdfAndParseMpreFromRoverUdf: UserDefinedFunction = udf((channelType: String, uri: String) => replaceMkgroupidMktypeAndParseMpreFromRover(channelType, uri))
  val getDateTimeUdf: UserDefinedFunction = udf((timestamp: Long) => tools.getDateTimeFromTimestamp(timestamp))
  val getPerfTrackNameValueUdf: UserDefinedFunction = udf((query: String) => tools.getPerfTrackNameValue(query))
  val getKeywordUdf: UserDefinedFunction = udf((query: String) => tools.getParamFromQuery(query, tools.keywordParams))
  val getDefaultNullNumParamValueFromUrlUdf: UserDefinedFunction = udf((query: String, key: String) => tools.getDefaultNullNumParamValueFromQuery(query, key))
  val getParamFromQueryUdf: UserDefinedFunction = udf((query: String, key: String) => tools.getParamValueFromQuery(query, key))
  val getUserMapIndUdf: UserDefinedFunction = udf((userId: String) => tools.getUserMapInd(userId))
  val needQueryCBToGetCguidUdf: UserDefinedFunction = udf((cguid: String, guid: String) => StringUtils.isEmpty(cguid) && StringUtils.isNotEmpty(guid))
  val getCguidUdf: UserDefinedFunction = udf((channelType: String, cguid: String, guid: String) => getCguid(channelType, cguid, guid))
  val judgeCGuidNotNullUdf: UserDefinedFunction = udf((channelType: String, cguid: String) => judgeCGuidNotNull(channelType, cguid))

  /**
   * override renameFiles to have special output file name for TD
   * @param outputDir final destination
   * @param workDir temp output
   * @param date current handled date
   * @return files array handled
   */
  def renameFiles(outputDir: String, workDir: String, date: String, channel: String): Array[String] = {
    // rename result to output dir
    val dateOutputPath = new Path("%s/%s/imkDump/%s".format(outputDir, channel, date))
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
        val target = new Path(dateOutputPath, "imk_rvr_trckng_" + tools.getOutPutFileDate + ".V4." + hostName + "." + channel + seq + ".dat.gz")
        logger.info("rename %s to %s".format(src.toString, target.toString))
        fs.rename(src, target)
        target.toString
      })
    files
  }

  /**
   * filder empty cguid traffic
   * @param channelType channel
   * @param cguid cguid
   * @return
   */
  def judgeCGuidNotNull(channelType:String, cguid: String): Boolean = {
    if (StringUtils.isEmpty(cguid)) {
      metrics.meter("imk.dump.nullCguid", 1, Field.of[String, AnyRef]("channelType", channelType))
      false
    } else {
      true
    }
  }

  /**
   * Campaign Manager changes the url template for all PLA accounts, replace adtype=pla and*adgroupid=65058347419* with
   * new parameter mkgroupid={adgroupid} and mktype={adtype}. Tracking’s MCS data pipeline job replace back to adtype
   * and adgroupid and persist into IMK so there wont be impact to downstream like data and science.
   * See <a href="https://jirap.corp.ebay.com/browse/XC-1464">replace landing page url and rvr_url's mktype and mkgroupid</a>
   * @param channelType channel
   * @param uri tracking url
   * @return new tracking url
   */
  def replaceMkgroupidMktype(channelType:String, uri: String): String = {
    var newUri = ""
    if (StringUtils.isNotEmpty(uri)) {
      try {
        newUri = uri.replace("mkgroupid", "adgroupid")
          .replace("mktype", "adtype")
      } catch {
        case e: Exception => {
          if (metrics != null) {
            metrics.meter("imk.dump.malformed", 1, Field.of[String, AnyRef]("channelType", channelType))
          }
          logger.warn("MalformedUrl", e)
        }
      }
    }

    newUri
  }

  /**
    * Parse mpre from if it's rover url
    * @param channelType channel type
    * @param uri uri
    * @return mpre
    */
  def replaceMkgroupidMktypeAndParseMpreFromRover(channelType:String, uri: String): String = {
    var newUri = replaceMkgroupidMktype(channelType, uri)
    // parse mpre if url is rover
    if (newUri.startsWith("http://rover.ebay.com") || newUri.startsWith("https://rover.ebay.com")) {
      val query = tools.getQueryString(newUri)
      val landingPageUrl = tools.getParamValueFromQuery(query, "mpre")
      if (StringUtils.isNotEmpty(landingPageUrl)) {
        try{
          newUri = URLDecoder.decode(landingPageUrl,"UTF-8")
        } catch {
          case e: Exception => {
            if(metrics != null) {
              metrics.meter("imk.dump.error.parseMpreFromRoverError", 1)
            }
            logger.warn("MalformedUrl", e)
          }
        }

      }
    }

    newUri
  }

  /**
   * get cguid
   * @param channelType channel
   * @param cguid cguid
   * @param guid guid
   * @return
   */
  def getCguid(channelType:String, cguid: String, guid: String): String = {
    if (StringUtils.isNotEmpty(cguid)) {
      cguid
    } else {
      metrics.meter("imk.dump.tryCguidByGuid", 1, Field.of[String, AnyRef]("channelType", channelType))
      if (guidCguidMap != null) {
        val result = guidCguidMap.getOrDefault(guid, "")
        if (StringUtils.isNotEmpty(result)) {
          metrics.meter("imk.dump.gotCguidByGuid", 1, Field.of[String, AnyRef]("channelType", channelType))
          result
        } else {
          guid
        }
      } else {
        guid
      }
    }
  }

  /**
   * async get cguid by guid list
   * @param channel channel
   * @param list guid list
   * @return
   */
  def batchGetCguids(channel:String, list: Array[String]): util.HashMap[String, String] = {
    val startTime = System.currentTimeMillis
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
      for (i <- 0 until jsonDocuments.size()) {
        val element = jsonDocuments.get(i)
        val cguidObject = gson.fromJson(String.valueOf(element.content()), classOf[Cguid])
        if (cguidObject != null) {
          res.put(element.id(), cguidObject.getCguid)
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase error while getting cguid by guid list" + e)
        metrics.meter("imk.dump.error.cbquery", 1, Field.of[String, AnyRef]("channelType", channel))
        // should we throw the exception and make the job fail?
        throw new Exception(e)
      }
    }
    CorpCouchbaseClient.returnClient(cacheClient)
    val endTime = System.currentTimeMillis
    metrics.mean("imk.dump.cb.latency", endTime - startTime, Field.of[String, AnyRef]("channelType", channel))
    res
  }

  /**
   * Check if this request is bot with brwsr_name
   * @param brwsrName alias for user agent
   * @return is bot or not
   */
  def isBotByUserAgent(brwsrName: String): Boolean = {
    isBot(brwsrName, userAgentBotDawgDictionary)
  }

  /**
   * Check if this request is bot with clnt_remote_ip
   * @param clntRemoteIp alias for remote ip
   * @return is bot or not
   */
  def isBotByIp(clntRemoteIp: String): Boolean = {
    isBot(clntRemoteIp, ipBotDawgDictionary)
  }

  def isBot(info: String, dawgDictionary: DawgDictionary): Boolean = {
    if (StringUtils.isEmpty(info)) {
      false
    } else {
      val dawg = new Dawg(dawgDictionary)
      val result = dawg.findAllWords(info.toLowerCase, false)
      if (result.isEmpty) {
        false
      } else {
        true
      }
    }
  }

  /**
   * imk crab transform for all channels, same with CrabTransformJob
   * @param imkDumpDfMap imk dump job output dataframe
   */
  def imkTransformJob(imkDumpDfMap: mutable.Map[String, mutable.Map[String, DataFrame]]): Unit = {
    fs.delete(new Path(imkETLTempDir), true)
    fs.delete(new Path(dtlETLTempDir), true)
    fs.delete(new Path(mgETLTempDir), true)

    val imkETLDfMap = mutable.Map[String, DataFrame]()
    val dtlETLDfMap = mutable.Map[String, DataFrame]()
    val mgETLDfMap = mutable.Map[String, DataFrame]()

    imkDumpDfMap.foreach(kv => {
      val channel = kv._1
      kv._2.foreach(singleChannelDf => {
        val date = singleChannelDf._1
        val imkDumpDf = singleChannelDf._2
        var imkTransformDf = imkDumpDf
          .filter(_.getAs[Long]("rvr_id") != null)
          .repartition(params.xidParallelNum)
          .withColumn("item_id", getApolloItemIdUdf(col("roi_item_id"), col("item_id")))
          .withColumn("user_id", getUserIdUdf(lit(channel), col("user_id"), col("cguid"), col("rvr_cmnd_type_cd")))
          .withColumn("mfe_id", getMfeIdUdf(col("mfe_name")))
          .withColumn("event_ts", setMessageLagUdf(lit(channel), col("event_ts")))
          .withColumn("mgvalue_rsn_cd", getMgvalueRsnCdUdf(col("mgvaluereason")))
          /** when spark read csv file in crab transform, all empty value will be converted to null. As a result, empty
           * dst_client_id will be converted to null firstly, and then converted to default value by
           * na.fill(schema_imk_table.defaultValues).  setDefaultValueForDstClientIdUdf is needed to keep same with
           * crab transform */
          .withColumn("dst_client_id", setDefaultValueForDstClientIdUdf(col("dst_client_id")))
          .na.fill(schema_imk_table.defaultValues).cache()

        // set default values for some columns
        schema_apollo.filterNotColumns(imkTransformDf.columns).foreach(e => {
          imkTransformDf = imkTransformDf.withColumn(e, lit(schema_apollo.defaultValues(e)))
        })
        schema_apollo_dtl.filterNotColumns(imkTransformDf.columns).foreach(e => {
          imkTransformDf = imkTransformDf.withColumn(e, lit(schema_apollo_dtl.defaultValues(e)))
        })
        schema_apollo_mg.filterNotColumns(imkTransformDf.columns).foreach(e => {
          imkTransformDf = imkTransformDf.withColumn(e, lit(schema_apollo_mg.defaultValues(e)))
        })

        // flush metrics for tasks
        import org.apache.spark.sql.catalyst.encoders.RowEncoder
        implicit val encoder: ExpressionEncoder[Row] = RowEncoder(imkTransformDf.schema)
        imkTransformDf = imkTransformDf.mapPartitions((iter: Iterator[Row]) => {
          if (metrics != null) {
            metrics.flush()
          }
          iter
        })

        // select imk columns
        val imkETLDf = imkTransformDf.select(schema_apollo.dfColumns: _*)

        // select dtl columns
        val dtlETLDf = imkTransformDf.select(schema_apollo_dtl.dfColumns: _*)

        // select mg columns
        val mgETLDf = imkTransformDf.select(schema_apollo_mg.dfColumns: _*).filter($"mgvalue" =!= "")

        if (imkETLDfMap.contains(date)) {
          imkETLDfMap.put(date, imkETLDfMap(date).union(imkETLDf))
          dtlETLDfMap.put(date, dtlETLDfMap(date).union(dtlETLDf))
          mgETLDfMap.put(date, mgETLDfMap(date).union(mgETLDf))
        } else {
          imkETLDfMap.put(date, imkETLDf)
          dtlETLDfMap.put(date, dtlETLDf)
          mgETLDfMap.put(date, mgETLDf)
        }
      })
    })

    saveToFile(imkETLDfMap, imkETLTempDir, imkETLOutputDir)
    saveToFile(dtlETLDfMap, dtlETLTempDir, dtlETLOutputDir)
    saveToFile(mgETLDfMap, mgETLTempDir, mgETLOutputDir)
  }

  def joinKw(channel: String, imkTransformDf: DataFrame): DataFrame = {
    if(channel.equals("PAID_SEARCH")) {
      val kwLKPDf = readFilesAsDF(params.kwDataDir).filter($"is_dup" === false)
      // select core data columns
      val paidSearchTransformDf = imkTransformDf.select(schema_apollo.dfColumns: _*).drop("kw_id")
      val smallJoinDf = paidSearchTransformDf.select("keyword", "rvr_id")
        .withColumnRenamed("rvr_id", "temp_rvr_id")
        .filter(kwIsNotEmptyUdf(col("keyword"))).distinct()

      // if input number is less, then we choose broadcast join to improve the performance
      // 1,000,000 tfs file format data approximately equals to 500MB
      var isBroadCast = smallJoinDf.count() <= 1000000
      logger.info("is broadCast %b".format(isBroadCast))
      val heavyJoinResultDf = getJoinedKwDf(smallJoinDf, kwLKPDf, isBroadCast)

      paidSearchTransformDf.join(heavyJoinResultDf, $"rvr_id" === $"temp_rvr_id", "left_outer")
        .withColumn("kw_id", setDefaultValueForKwIdUdf(col("kw_id")))
    } else {
      imkTransformDf
    }
  }

  // join keyword table
  def getJoinedKwDf(smallJoinDf: DataFrame, kwLKPDf: DataFrame, isBroadcast: Boolean) : DataFrame = {
    if(isBroadcast) {
      kwLKPDf.join(broadcast(smallJoinDf), $"keyword" === $"kw", "inner")
        .withColumnRenamed("keyword", "temp_kw")
    } else {
      kwLKPDf.join(smallJoinDf, $"keyword" === $"kw", "inner")
        .withColumnRenamed("keyword", "temp_kw")
    }
  }

  private def saveToFile(dfMap: mutable.Map[String, DataFrame], tempDir: String, outputDir: String): Unit = {
    dfMap.foreach(kv => {
      val date = kv._1
      val df = kv._2.repartition(params.partitions)
      params.outputFormat match {
        case "sequence" =>
          df.rdd.map(row => ("", row.mkString("\u007F"))).saveAsSequenceFile(tempDir, compressCodec)
        case "parquet" => saveDFToFiles(df, tempDir)
        case _ => throw new Exception("Invalid output format %s.".format(params.outputFormat))
      }
      simpleRenameFiles(tempDir, outputDir, date)
      // when save as sequence file, the output dir should not be existed.
      fs.delete(new Path(tempDir), true)
    })
  }

  val getApolloItemIdUdf: UserDefinedFunction = udf((roi_item_id: String, item_id: String) => getApolloItemId(roi_item_id, item_id))
  val getMfeIdUdf: UserDefinedFunction = udf((mfe_name: String) => getMfeIdByMfeName(mfe_name))
  val getMgvalueRsnCdUdf: UserDefinedFunction = udf((mgvaluereason: String) => getMgvalueRsnCd(mgvaluereason))
  val setDefaultValueForKwIdUdf: UserDefinedFunction = udf((kw_id: String) => {
    if (StringUtils.isEmpty(kw_id)) {
      "-999"
    } else {
      kw_id
    }
  })
  val setDefaultValueForDstClientIdUdf: UserDefinedFunction = udf((dstClientId: String) => {
    if (StringUtils.isEmpty(dstClientId)) {
      "0"
    } else {
      dstClientId
    }
  })
  val kwIsNotEmptyUdf: UserDefinedFunction = udf((keyword: String) => StringUtils.isNotEmpty(keyword))
  val getUserIdUdf: UserDefinedFunction = udf((channelType:String, userId: String, cguid: String, cmndType: String) => getUserIdByCguid(channelType, userId, cguid, cmndType))
  val setMessageLagUdf: UserDefinedFunction = udf((channelType:String, eventTs: String) => setMessageLag(channelType, eventTs))
  /**
   * set message lag
   * @param channelType channel
   * @param eventTs message event_ts
   * @return
   */
  def setMessageLag(channelType:String, eventTs: String): String = {
    if (StringUtils.isNotEmpty(eventTs)) {
      try{
        val messageDt = dateFormat.parse(eventTs)
        val nowDt = new Date()
        metrics.mean("imk.transform.messageLag", nowDt.getTime - messageDt.getTime, Field.of[String, AnyRef]("channelType", channelType))
      } catch {
        case e:Exception => {
          logger.warn("parse event ts error", e)
        }
      }
    }
    eventTs
  }

  /**
   * set value for userid.
   * 1, use origin userid if it's not empty.
   * 2, use cguid to call ERS to get user id
   * @param channelType channel
   * @param userId origin user id
   * @param cguid cguid
   * @return user id
   */
  def getUserIdByCguid(channelType:String, userId: String, cguid: String, cmndType: String): String = {
    if (StringUtils.isEmpty(cmndType) || cmndType.equals("4")) {
      return userId
    }
    var result = userId
    if (StringUtils.isEmpty(userId) || userId.equals("0")) {
      if (StringUtils.isNotEmpty(cguid)) {
        try{
          metrics.meter("imk.transform.XidTryGetUserId", 1, Field.of[String, AnyRef]("channelType", channelType))
          val xid = xidRequest("cguid", cguid)
          if (xid.accounts.nonEmpty) {
            metrics.meter("imk.transform.XidGotUserId", 1, Field.of[String, AnyRef]("channelType", channelType))
            result = xid.accounts.head
          }
        } catch {
          case e: Exception => {
            logger.warn("call xid error" + e.printStackTrace())
          }
        }
      }
    }
    result
  }

  /**
   * call xid service to get userid by cguid
   * @param idType cguid, gadid, idfa, account
   * @param id
   * @return
   */
  def xidRequest(idType: String, id: String): MyID = {
    Http(s"http://$xidHost/anyid/v1/$idType/$id")
      .header("X-EBAY-CONSUMER-ID", xidConsumerId)
      .header("X-EBAY-CLIENT-ID", xidClientId)
      .timeout(xidConnectTimeout, xidReadTimeout)
      .asString
      .body
      .parseJson
      .convertTo[XIDResponse]
      .toMyID()
  }

  /**
   * set apollo item_id filed by tfs item_id and roi_item_id
   * @param roi_item_id roi_item_id
   * @param item_id item_id
   * @return apollo item_id
   */
  def getApolloItemId(roi_item_id: String, item_id: String): String = {
    if (StringUtils.isNotEmpty(roi_item_id) && StringUtils.isNumeric(roi_item_id) && roi_item_id.toLong != -999) {
      roi_item_id
    } else if (StringUtils.isNotEmpty(item_id) && item_id.length <= 18) {
      item_id
    } else{
      ""
    }
  }

  /**
   * get mfe id by mfe name
   * @param mfeName mfe name
   * @return
   */
  def getMfeIdByMfeName(mfeName: String): String = {
    if (StringUtils.isNotEmpty(mfeName)) {
      mfe_name_id_map.getOrElse(mfeName, "-999")
    } else {
      "-999"
    }
  }

  def getMgvalueRsnCd(mgvaluereason: String): String = {
    if ("4".equalsIgnoreCase(mgvaluereason) || "BOT".equalsIgnoreCase(mgvaluereason)) {
      "4"
    } else {
      ""
    }
  }

  /**
   * Move file from temp dir to final dir for hadoop
   * @param workDir temp dir
   * @param outputDir final output dir
   * @param date date
   */
  def simpleRenameFiles(workDir: String, outputDir: String, date: String): Unit = {
    val status = fs.listStatus(new Path(workDir))
    status
      .filter(path => path.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format swi._2).replace(" ", "0")
        // chocolate_appid_seq
        val fileName = params.transformedPrefix + date + "_" + sc.applicationId + "_" + seq
        logger.info("rename %s to %s".format(src.toString, outputDir + "/" + fileName))
        fs.rename(new Path(src.toString), new Path(outputDir + "/" + fileName))
      })
  }

}
