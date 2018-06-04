package com.ebay.traffic.chocolate.sparknrt.reporting

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, lit, min}

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

  // Get capping metadata for EPN, while getting dedupe metadata for Display.
  @transient lazy val metadata = {
    Metadata(params.workDir, params.channel, if (params.channel == ChannelType.EPN.toString) MetadataEnum.capping else MetadataEnum.dedupe)
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

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
    * Format - [publisher id]_[date]_[action]_[MOBILE or DESKTOP]_[RAW or FILTERED]
    */
  def getUniqueKey(publisherId: String, date: String, action: String, isMob: Boolean, isFiltered: Boolean): String = {
    if (isMob && isFiltered)
      publisherId + "_" + date + "_" + action + "_MOBILE_FILTERED"
    else if (isMob && !isFiltered)
      publisherId + "_" + date + "_" + action + "_MOBILE_RAW"
    else if (!isMob && isFiltered)
      publisherId + "_" + date + "_" + action + "_DESKTOP_FILTERED"
    else
      publisherId + "_" + date + "_" + action + "_DESKTOP_RAW"
  }

  def upsertCouchbase(date: String, iter: Iterator[Row]): Unit = {
    while (iter.hasNext) {
      val row = iter.next()

      val key = getUniqueKey(row.getAs("publisher_id").toString,
        date,
        row.getAs("channel_action"),
        row.getAs("is_mob"),
        row.getAs("is_filtered"))

      val mapData = Map("timestamp" -> row.getAs("timestamp"), "count" -> row.getAs("count"))

      CouchbaseClient.upsert(key, mapData)
    }
  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }

  //import spark.implicits._

  override def run(): Unit = {

    // 1. load metafiles
    val dedupeOutputMeta = metadata.readDedupeOutputMeta()

    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2

      datesFiles.foreach(datesFile => {
        // 2. load DataFrame
        val date = getDate(datesFile._1)
        val df = readFilesAsDFEx(datesFile._2)

        // 3. do aggregation (count) - click, impression, viewable for both desktop and mobile

        // Raw + Desktop
        val df1 = df.filter(row => !checkMobileUserAgent(row.getAs("request_headers")))
          .groupBy("publisher_id", "channel_action")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_mob", lit(false))
          .withColumn("is_filtered", lit(false))

        // Raw + Mobile
        val df2 = df.filter(row => checkMobileUserAgent(row.getAs("request_headers")))
          .groupBy("publisher_id", "channel_action")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_mob", lit(true))
          .withColumn("is_filtered", lit(false))

        // Filtered + Desktop
        val df3 = df.filter(row => !checkMobileUserAgent(row.getAs("request_headers")))
          .where("rt_rule_flags == 0 and nrt_rule_flags == 0")
          .groupBy("publisher_id", "channel_action")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_mob", lit(false))
          .withColumn("is_filtered", lit(true))

        // Filtered + Mobile
        val df4 = df.filter(row => checkMobileUserAgent(row.getAs("request_headers")))
          .where("rt_rule_flags == 0 and nrt_rule_flags == 0")
          .groupBy("publisher_id", "channel_action")
          .agg(count("snapshot_id").alias("count"), min("timestamp").alias("timestamp"))
          .withColumn("is_mob", lit(true))
          .withColumn("is_filtered", lit(true))

        val resultDF = df1 union df2 union df3 union df4

        // 4. persist the result into Couchbase
        resultDF.foreachPartition(iter => {
          upsertCouchbase(date, iter)
        })
      })

      // 5. delete metafile that is processed
      metadata.deleteDedupeOutputMeta(metaIter._1)
    })
  }
}
