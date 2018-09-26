package com.ebay.traffic.chocolate.sparknrt.reporting

import java.text.SimpleDateFormat
import java.util.Properties

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

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

  @transient lazy val metadata = {
    var usage = MetadataEnum.dedupe
    if (params.channel == ChannelType.EPN.toString) {
      usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("reporting.upstream.epn"))
    } else if (params.channel == ChannelType.DISPLAY.toString) {
      usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("reporting.upstream.display"))
    }
    Metadata(params.workDir, params.channel, usage)
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  lazy val archiveDir = params.archiveDir + "/" + params.channel + "/reporting/"

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

  def upsertCouchbase(date: String, iter: Iterator[Row], isCampaignReport: Boolean): Unit = {
    while (iter.hasNext) {
      val row = iter.next()

      var prefix = "PUBLISHER_" + row.getAs("publisher_id").toString
      if (isCampaignReport) {
        prefix += "_CAMPAIGN_" + row.getAs("campaign_id").toString
      }

      val key = getUniqueKey(
        prefix,
        date,
        row.getAs("channel_action"),
        row.getAs("is_mob"),
        row.getAs("is_filtered"))

      val mapData = Map("timestamp" -> row.getAs("timestamp"), "count" -> row.getAs("count"))

      CorpCouchbaseClient.upsertMap(key, mapData)
    }
  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }

  import spark.implicits._

  override def run(): Unit = {

    // 1. load metafiles
    logger.info("load metadata...")
    val dedupeOutputMeta = metadata.readDedupeOutputMeta()

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
        publisherDf.foreachPartition(iter => upsertCouchbase(date, iter, false))

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

        val campaignDf = campaignDf1 union campaignDf2

        // 4. persist the result into Couchbase
        logger.info("persist campaign report into Couchbase...")
        campaignDf.foreachPartition(iter => upsertCouchbase(date, iter, true))
      })

      // 5. archive metafile that is processed for replay
      logger.info(s"archive metafile=$file")
      archiveMetafile(file, archiveDir)
    })
  }
}
