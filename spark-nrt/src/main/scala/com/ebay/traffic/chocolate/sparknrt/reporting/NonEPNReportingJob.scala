package com.ebay.traffic.chocolate.sparknrt.reporting

import java.net.URI

import com.ebay.traffic.monitoring.{ESReporting, Field, Reporting}
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkDump.TableSchema
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import collection.JavaConverters._

object NonEPNReportingJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = NonEPNParameter(args)

    val job = new NonEPNReportingJob(params)

    job.run()
    job.stop()
  }
}

class NonEPNReportingJob (params: NonEPNParameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  @transient override lazy val fs: FileSystem = {
    val fs = FileSystem.get(URI.create(params.workDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val esReporting: Reporting = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESReporting.init("chocolate-report-", params.elasticsearchUrl)
      ESReporting.getInstance()
    } else {
      throw new Exception("no es url")
    }
  }

  @transient lazy val schema_reporting = TableSchema("df_reporting.json")

  @transient lazy val metadata: Metadata = Metadata(params.workDir, params.channel, MetadataEnum.capping)

  lazy val archiveDir: String = params.archiveDir + "/" + params.channel + "/reporting"
  lazy private val knownRoverCommand = Set("rover", "roverimp", "ar")

  //import spark.implicits._

  override def run(): Unit = {
    logger.info("load metadata...")
    var dedupeOutputMeta = metadata.readDedupeOutputMeta()
    if (dedupeOutputMeta.length > params.batchSize) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, params.batchSize)
    }
    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        val dateFiles = datesFile._2.map(file => {
          if (file.startsWith("hdfs")){
            file
          } else {
            params.hdfsUri + file
          }
        })
        val df = readFilesAsDFEx(dateFiles)
          .filter(filterActionUdf(col("channel_action"), lit(params.filterAction)))
          .withColumn("id", col("snapshot_id"))
          .withColumn("timestamp", col("timestamp"))
          .withColumn("is_mob", isMobUdf(col("request_headers")))
          .withColumn("is_filtered", isFilteredUdf(col("rt_rule_flags")))
          .withColumn("publisher_id", col("publisher_id"))
          .withColumn("campaign_id", col("campaign_id"))
          .withColumn("rotation_id", getRotationIdUdf(col("uri")))
          .withColumn("channel", lit(params.channel))
          .withColumn("channel_action", getChannelActionUdf(col("channel_action")))
          .select(schema_reporting.dfColumns: _*)

        if (params.channel.equals("EPN")) {
          val resultDf = df.groupBy( "channel_action", "is_mob", "is_filtered", "publisher_id", "campaign_id")
            .agg(count("id").alias("count"), min("timestamp").alias("timestamp"))
            .withColumn("channel", lit(params.channel))

          resultDf.foreach(row => {
            upsertElasticSearch(row)
          })
        } else {
          val resultDf = df.groupBy( "channel_action", "is_mob", "is_filtered", "rotation_id")
            .agg(count("id").alias("count"), min("timestamp").alias("timestamp"))
            .withColumn("channel", lit(params.channel))

          resultDf.foreach(row => {
            upsertElasticSearch(row)
          })
        }

      })

      // 5. archive metafile that is processed for replay
      logger.info(s"archive metafile=$file")
      archiveMetafile(file, archiveDir)
    })

  }

  val isMobUdf: UserDefinedFunction = udf((requestHeaders: String) => checkMobileUserAgent(requestHeaders))
  val isFilteredUdf: UserDefinedFunction = udf((rtFlag: String) => !rtFlag.equals("0"))
  val getRotationIdUdf: UserDefinedFunction = udf((uri: String) => getRotationId(uri))
  val getChannelActionUdf: UserDefinedFunction = udf((channelAction: String) => channelAction.toLowerCase)
  val filterActionUdf: UserDefinedFunction = udf((channelAction: String, filterAction: String) => isFilterAction(channelAction, filterAction))
  /**
    * upsert record to ES
    * @param row record
    */
  def upsertElasticSearch(row: Row): Unit = {
    retry(3) {
      val docId = row.mkString("-")

      val fieldsMap = row.getValuesMap(row.schema.fieldNames)
      var fields = new Array[Field[String, AnyRef]](0)

      fieldsMap.foreach(field => {
        fields = fields :+ Field.of[String, AnyRef](field._1, field._2)
      })

      esReporting.send("CHOCOLATE_REPORT", row.getAs("count").toString.toLong, docId,
        row.getAs("timestamp").toString.toLong, fields:_*
      )
    }
  }

  def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e:Exception =>
        if (n > 1) retry(n - 1)(fn)
        else throw e
    }
  }

  /**
    * only get wanted action type
    * @param channelAction real action
    * @param filterAction action from param
    * @return
    */
  def isFilterAction(channelAction: String, filterAction: String): Boolean ={
    if (filterAction.equals("all")) {
      true
    } else {
      channelAction.toLowerCase.equals(filterAction.toLowerCase)
    }
  }

  /**
    * Get rotationId from rover URL. rotationId is not available in design
    * of c.ebay.com API, so a default value -1 will be returned.
    */
  def getRotationId(uri: String): String = {

    // remove protocol and query string...
    var indexOfQueryString = uri.indexOf("?")
    if (indexOfQueryString == -1) {
      indexOfQueryString = uri.length
    }

    val strimUri =
      if (uri.startsWith("http://")) uri.substring(7, indexOfQueryString)
      else if (uri.startsWith("https://")) uri.substring(8, indexOfQueryString)
      else uri

    val splitted = strimUri.split("/")

    if ((splitted == null || splitted.length < 4) ||
      !knownRoverCommand.contains(splitted(splitted.length - 4)) || // check rover command
      StringUtils.isEmpty(splitted(splitted.length - 2))) { // check rotationId
      "-1" // default to -1 as unknown rotationId
    } else {
      val rotationId = splitted(splitted.length - 2)
      // It is often seen that some uri is malformed and has a incorrect rotation id.
      if (checkRotationId(rotationId)) {
        rotationId
      } else {
        "-1"
      }
    }
  }

  /**
    * Check whether current event is sent from mobile by check User-Agent.
    */
  def checkMobileUserAgent(requestHeaders: String): Boolean = {
    val parts = requestHeaders.split("\\|")
    for (i <- 0 until parts.length) {
      val part = parts(i)
      val splitter = part.indexOf(':')
      if (splitter > 0 && splitter + 1 < part.length) {
        val key = part.substring(0, splitter).trim
        if (key.equalsIgnoreCase("User-Agent") && part.contains("Mobi")) {
          return true
        }
      }
    }
    false
  }

  def checkRotationId(rotationId: String): Boolean = {
    // According to the query, the largest length of a valid rotation id is <=21,
    // let's assume it <=25 for simplification. Besides that, rotation id should
    // contain numeric char only after the removal of dash '-'.
    rotationId.length <= 25 && StringUtils.isNumeric(rotationId.replace("-", ""))
  }
}
