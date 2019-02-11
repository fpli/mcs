package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.InetAddress
import java.util.Properties

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkDump.Tools.keywordParams
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

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

  lazy val sparkDir: String = params.workDir + "/" + params.channel + "/spark/"

  lazy val METRICS_INDEX_PREFIX = "chocolate-metrics-"

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

  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
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

    // metafiles for output data
    val suffix = properties.getProperty("imkdump.meta.output.suffix")
    var suffixArray: Array[String] = Array()
    if (StringUtils.isNotEmpty(suffix)) {
      suffixArray = suffix.split(",")
    }

    var dedupeOutputMeta = inputMetadata.readDedupeOutputMeta(".imk")
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

        val imkDf = imkDumpCore(df).repartition(params.partitions)

        saveDFToFiles(imkDf, sparkDir, "gzip", "csv", "bel")

        val files = renameFiles(outputDir, sparkDir, date)
        DateFiles(date, files)
      }).toArray

      outputMetadata.writeDedupeOutputMeta(MetaFiles(outputMetas), suffixArray)
      inputMetadata.deleteDedupeOutputMeta(metaFile)

      metrics.meter("imk.dump.spark.out", dataFiles.size)
    })
    metrics.flush()
    metrics.close()
  }

  def imkDumpCore(df: DataFrame): DataFrame = {
    var imkDf = df
      .withColumn("batch_id", getBatchIdUdf())
      .withColumn("rvr_id", col("short_snapshot_id"))
      .withColumn("event_dt", getDateUdf(col("timestamp")))
      .withColumn("rvr_cmnd_type_cd", getCmndTypeUdf(col("channel_action")))
      .withColumn("rvr_chnl_type_cd", getParamFromQueryUdf(col("uri"), lit("cid")))
      .withColumn("clnt_remote_ip", col("remote_ip"))
      .withColumn("brwsr_type_id", getBrowserTypeUdf(col("user_agent")))
      .withColumn("brwsr_name", col("user_agent"))
      .withColumn("rfrr_dmn_name", getLandingPageDomainUdf(col("referer")))
      .withColumn("rfrr_url", col("referer"))
      .withColumn("src_rotation_id", col("src_rotation_id"))
      .withColumn("dst_rotation_id", col("dst_rotation_id"))
      .withColumn("dst_client_id", getClientIdUdf(col("uri")))
      .withColumn("lndng_page_dmn_name", getLandingPageDomainUdf(col("uri")))
      .withColumn("lndng_page_url", col("uri"))
      .withColumn("user_query", getUserQueryUdf(col("uri")))
      .withColumn("rule_bit_flag_strng", col("rt_rule_flags"))
      .withColumn("event_ts", getDateTimeUdf(col("timestamp")))
      .withColumn("perf_track_name_value", getUserQueryUdf(col("uri")))
      .withColumn("keyword", getKeywordUdf(col("uri")))
      .withColumn("mt_id", getDefaultNullNumParamValueFromUrlUdf(col("uri"), lit("mt_id")))
      .withColumn("crlp", getParamFromQueryUdf(col("uri"), lit("crlp")))
      .withColumn("user_map_ind", getUserMapIndUdf(col("user_id")))
      .withColumn("item_id", getItemIdUdf(col("uri")))

    for (i <- 1 to 20) {
      val columnName = "flex_field_" + i
      val paramName = "ff" + i
      imkDf = imkDf.withColumn(columnName, getParamFromQueryUdf(col(paramName)))
    }

    val schema_imk_table = TableSchema("df_imk.json")
    schema_imk_table.filterNotColumns(imkDf.columns).foreach(e => {
      imkDf = imkDf.withColumn(e, lit(schema_imk_table.defaultValues(e)))
    })
    imkDf.select(schema_imk_table.columnNames.head, schema_imk_table.columnNames.tail: _*).withColumn("delimeter", lit(""))
  }

  val getUserQueryUdf: UserDefinedFunction = udf((uri: String) => Tools.getQueryString(uri))
  val getDefaultNullNumParamValueFromUrlUdf: UserDefinedFunction = udf((header: String, key: String) => Tools.getDefaultNullNumParamValueFromUrl(header, key))
  val getDateTimeUdf: UserDefinedFunction = udf((timestamp: Long) => Tools.getDateTimeFromTimestamp(timestamp))
  val getDateUdf: UserDefinedFunction = udf((timestamp: Long) => Tools.getDateFromTimestamp(timestamp))
  val getClientIdUdf: UserDefinedFunction = udf((uri: String) => Tools.getClientIdFromRotationId(Tools.getParamValueFromUrl(uri, "rid")))
  val getItemIdUdf: UserDefinedFunction = udf((uri: String) => Tools.getItemIdFromUri(uri))
  val getKeywordUdf: UserDefinedFunction = udf((uri: String) => Tools.getParamFromQuery(uri, keywordParams))
  val getLandingPageDomainUdf: UserDefinedFunction = udf((uri: String) => Tools.getDomain(uri))
  val getUserMapIndUdf: UserDefinedFunction = udf((userId: String) => Tools.getUserMapInd(userId))
  val getParamFromQueryUdf: UserDefinedFunction = udf((uri: String, key: String) => Tools.getParamValueFromUrl(uri, key))
  val getBrowserTypeUdf: UserDefinedFunction = udf((userAgent: String) => Tools.getBrowserType(userAgent))
  val getCmndTypeUdf: UserDefinedFunction = udf((channelType: String) => Tools.getCommandType(channelType))
  val getBatchIdUdf: UserDefinedFunction = udf(() => Tools.getBatchId)

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
    val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")
      .map(swi => {
        val src = swi.getPath
//        imk_rvr_trckng_????????_??????.V4.*dat.gz
        val target = new Path(dateOutputPath, "imk_rvr_trckng_" + Tools.getOutPutFileDate + ".V4." + hostName + "." + params.channel + ".dat.gz")
        logger.info("Rename from: " + src.toString + " to: " + target.toString)
        fs.rename(src, target)
        target.toString
      })
    files
  }

}
