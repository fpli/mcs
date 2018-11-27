package com.ebay.traffic.chocolate.sparknrt.reporting

import java.util.Properties

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.monitoring.{ESReporting, Field, Reporting}
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import collection.JavaConverters._

/**
  * Created by weibdai on 5/19/18.
  */
object ReportingJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ReportingJob(params)

    job.run()
    job.stop()
  }
}

class ReportingJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  @transient var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("reporting.properties"))
    properties
  }

  @transient lazy val metadata: Metadata = {
    var usage = MetadataEnum.dedupe
    if (params.channel == ChannelType.EPN.toString) {
      usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("reporting.upstream.epn"))
    } else if (params.channel == ChannelType.DISPLAY.toString) {
      usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("reporting.upstream.display"))
    }
    Metadata(params.workDir, params.channel, usage)
  }

  @transient lazy val batchSize: Int = {
    val batchSize = properties.getProperty("reporting.metafile.batchsize")
    if (StringUtils.isNumeric(batchSize)) {
      Integer.parseInt(batchSize)
    } else {
      10 // default to 10 metafiles
    }
  }

  @transient lazy val esReporting: Reporting = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESReporting.init("chocolate-report-", params.elasticsearchUrl)
      ESReporting.getInstance()
    } else {
      throw new Exception("no es url")
    }
  }

  lazy val archiveDir: String = params.archiveDir + "/" + params.channel + "/reporting/"

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

  lazy private val knownRoverCommand = Set("rover", "roverimp", "ar")

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
      !splitted(splitted.length - 1).equals("4") || // check channelId
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

  def checkRotationId(rotationId: String): Boolean = {
    // According to the query, the largest length of a valid rotation id is <=21,
    // let's assume it <=25 for simplification. Besides that, rotation id should
    // contain numeric char only after the removal of dash '-'.
    rotationId.length <= 25 && StringUtils.isNumeric(rotationId.replace("-", ""))
  }

  /**
    * Generate unique key for a Couchbase record.
    *
    * 1. Format for publisher based report:
    * [PUBLISHER]_[publisher id]_[date]_[action]_[MOBILE or DESKTOP]_[RAW or FILTERED]
    *
    * 2. Format for campaign based report:
    * [PUBLISHER]_[publisher id]_CAMPAIGN_[campaign id]_[date]_[action]_[MOBILE or DESKTOP]_[RAW or FILTERED]
    */
  def getUniqueKey(prefix: String,
                   date: String,
                   action: String,
                   isMob: Boolean,
                   isFiltered: Boolean): String = {
    val key = prefix + "_" + date + "_" + action
    if (isMob && isFiltered)
      key + "_MOBILE_FILTERED"
    else if (isMob && !isFiltered)
      key + "_MOBILE_RAW"
    else if (!isMob && isFiltered)
      key + "_DESKTOP_FILTERED"
    else
      key + "_DESKTOP_RAW"
  }

  def upsertCouchbase(date: String, row: Row, isCampaignReport: Boolean): Unit = {
    var prefix = "PUBLISHER_" + row.getAs("publisher_id").toString
    if (isCampaignReport) {
      prefix += "_CAMPAIGN_" + row.getAs("campaign_id").toString
    }

    upsertCouchbase(prefix, date, row)

  }

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

  def upsertCouchbase(date: String, row: Row): Unit = {
    val prefix = "ROTATION_" + row.getAs("rotation_id").toString
    upsertCouchbase(prefix, date, row)
  }

  def upsertCouchbase(prefix: String, date: String, row: Row): Unit = {
    val key = getUniqueKey(
      prefix,
      date,
      row.getAs("channel_action"),
      row.getAs("is_mob"),
      row.getAs("is_filtered"))

    val mapData = Map("timestamp" -> row.getAs("timestamp"), "count" -> row.getAs("count"))

    CorpCouchbaseClient.upsertMap(key, mapData)
  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }

  //import spark.implicits._

  override def run(): Unit = {
    CorpCouchbaseClient.initDataSource(properties.getProperty("reporting.cbDataSource"))
    if (params.channel == ChannelType.EPN.toString) {
      logger.info("generate report for EPN channel.")
      generateReportForEPN()
    } else if (params.channel == ChannelType.DISPLAY.toString) {
      logger.info("generate report for Display channel.")
      generateReportForDisplay()
    } else {
      logger.warn(s"${params.channel} channel is not supported yet!")
    }
  }

  def generateReportForEPN(): Unit = {
    // 1. load metafiles
    logger.info("load metadata...")
    var dedupeOutputMeta = metadata.readDedupeOutputMeta()
    if (dedupeOutputMeta.length > batchSize) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, batchSize)
    }

    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2

      datesFiles.foreach(datesFile => {
        // 2. load DataFrame
        val date = getDate(datesFile._1)
        val df = readFilesAsDFEx(datesFile._2)
        logger.info("load DataFrame, date=" + date +", with files=" + datesFile._2.mkString(","))

        // 3. do aggregation (count) - click, impression, viewable for both desktop and mobile
        val isMobUdf = udf((requestHeaders: String) => checkMobileUserAgent(requestHeaders))

        val commonDf = df.withColumn("is_mob", isMobUdf(col("request_headers"))).cache()
        val filteredDf = commonDf.where("rt_rule_flags == 0 and nrt_rule_flags == 0").cache()

        // publisher based report...
        logger.info("generate publisher based report...")

        // Raw + Desktop and Mobile
        val publisherDf1 = commonDf
          .groupBy("publisher_id", "channel_action", "is_mob")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_filtered", lit(false))

        // Filtered + Desktop and Mobile
        val publisherDf2 = filteredDf
          .groupBy("publisher_id", "channel_action", "is_mob")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_filtered", lit(true))

        val publisherDf = publisherDf1 union publisherDf2

        // 4. persist the result into Couchbase
        logger.info("persist publisher report into Couchbase...")
        publisherDf.foreachPartition(iter => {
          while (iter.hasNext) {
            upsertCouchbase(date, iter.next(), false)
          }
        })

        // campaign based report...
        logger.info("generate campaign based report...")

        // Raw + Desktop and Mobile
        val campaignDf1 = commonDf
          .groupBy("publisher_id", "campaign_id", "channel_action", "is_mob")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_filtered", lit(false))

        // Filtered + Desktop and Mobile
        val campaignDf2 = filteredDf
          .groupBy("publisher_id", "campaign_id", "channel_action", "is_mob")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_filtered", lit(true))

        val campaignDf = (campaignDf1 union campaignDf2).withColumn("channel_type", lit(ChannelType.EPN.toString))

        // 4. persist the result into Couchbase and elasticsearch
        logger.info("persist campaign report into Couchbase and elasticsearch...")
        campaignDf.foreachPartition(iter => {
          while (iter.hasNext) {
            val row = iter.next()
            upsertCouchbase(date, row, true)
            upsertElasticSearch(row)
          }
        })

      })

      // 5. archive metafile that is processed for replay
      logger.info(s"archive metafile=$file")
      archiveMetafile(file, archiveDir)
    })
  }

  def generateReportForDisplay(): Unit = {
    // 1. load metafiles
    logger.info("load metadata...")
    var dedupeOutputMeta = metadata.readDedupeOutputMeta()
    if (dedupeOutputMeta.length > batchSize) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, batchSize)
    }

    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2

      datesFiles.foreach(datesFile => {
        // 2. load DataFrame
        val date = getDate(datesFile._1)
        val df = readFilesAsDFEx(datesFile._2)
        logger.info("load DataFrame, date=" + date +", with files=" + datesFile._2.mkString(","))

        // 3. do aggregation (count) - click, impression, viewable for both desktop and mobile
        val isMobUdf = udf((requestHeaders: String) => checkMobileUserAgent(requestHeaders))
        val getRotationIdUdf = udf((uri: String) => getRotationId(uri.trim))

        val commonDf = df
          .withColumn("is_mob", isMobUdf(col("request_headers")))
          .withColumn("rotation_id", getRotationIdUdf(col("uri")))
          .cache()
        val filteredDf = commonDf.where("rt_rule_flags == 0 and nrt_rule_flags == 0").cache()

        // rotationId based report...
        logger.info("generate rotationId based report...")

        // Raw + Desktop and Mobile
        val rotationDf1 = commonDf
          .groupBy("rotation_id", "channel_action", "is_mob")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_filtered", lit(false))

        // Filtered + Desktop and Mobile
        val rotationDf2 = filteredDf
          .groupBy("rotation_id", "channel_action", "is_mob")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_filtered", lit(true))

        val publisherDf = (rotationDf1 union rotationDf2).withColumn("channel_type", lit(ChannelType.DISPLAY.toString))

        // 4. persist the result into Couchbase and elasticsearch
        logger.info("persist report into Couchbase and elasticsearch...")
        publisherDf.foreachPartition(iter => {
          while (iter.hasNext) {
            val row = iter.next()
            upsertCouchbase(date, row)
            upsertElasticSearch(row)
          }
        })
      })

      // 5. archive metafile that is processed for replay
      logger.info(s"archive metafile=$file")
      archiveMetafile(file, archiveDir)
    })
  }
}
