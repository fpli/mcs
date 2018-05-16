package com.ebay.traffic.chocolate.sparknrt.capping.rules

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.Parameter
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
  * Created by xiangli4 on 4/8/18.
  */
class IPPubCappingRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
    extends GenericCountRule(params: Parameter, bit: Long, dateFiles: DateFiles,  cappingRuleJobObj: BaseSparkJob) {

  import cappingRuleJobObj.spark.implicits._

  lazy val ruleName = "IPPubCappingRule_" + window
  lazy val ruleType = "timeWindow_" + window
  lazy val threshold = getThreshold(ruleName)
  lazy val timeWindow = getTimeWindow(ruleType)
  override lazy val fileName = "/ipPub_" + window + "/"

  override def test(): DataFrame = {

    // filter click only, count ip and save to tmp file
    var dfIP = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
        .filter($"channel_action" === "CLICK")
        .filter($"publisher_id" =!= -1)
    val head = dfIP.take(1)
    if (head.length == 0) {
      var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files).withColumn("capping", lit(0l))
      df
    }
    else {
      val firstRow = head(0)
      val timestamp = dfIP.select($"timestamp").first().getLong(0)

      dfIP = dfIP.select(split($"request_headers", "X-eBay-Client-IP: ")(1).alias("tmpIP"), $"publisher_id")
          .select(split($"tmpIP", """\|""")(0).alias("IP"), $"publisher_id")
          .groupBy($"IP", $"publisher_id").agg(count(lit(1)).alias("count"))
          .drop($"request_headers")
          .drop($"tmpIP")
      System.out.println("dfIP")
      dfIP.show

      // reduce the number of ip count file to 1, and rename file name to include timestamp
      reduceAndRename(dfIP, timestamp)

      // IP rule
      var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
          .withColumn("tmpIP", split($"request_headers", "X-eBay-Client-IP: ")(1))
          .withColumn("IP_1", when($"channel_action" === "CLICK", split($"tmpIP", """\|""")(0)).otherwise("NA"))
          .drop($"tmpIP")
      System.out.println("df")
      df.show

      val ipCountPath = readCountData(timestamp, timeWindow)

      dfIP = cappingRuleJobObj.readFilesAsDFEx(ipCountPath.toArray).groupBy($"IP", $"publisher_id").agg(sum($"count") as "amnt").filter($"amnt" >= threshold)
          .withColumn("capping", lit(cappingBit)).drop($"count").drop($"amnt")
      System.out.println("dfIP")
      dfIP.show

      df = df.join(dfIP, $"IP_1" === $"IP" && df.col("publisher_id") === dfIP.col("publisher_id"), "left_outer")
          .select(df.col("*"), $"capping")
          .drop("IP_1")
          .drop("IP")
      System.out.println("df")
      df.show
      df
    }
  }
}
