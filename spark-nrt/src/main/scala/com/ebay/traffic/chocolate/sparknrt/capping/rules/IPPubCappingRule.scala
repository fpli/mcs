package com.ebay.traffic.chocolate.sparknrt.capping.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.Parameter
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Created by jialili1 on 5/10/18.
  */
class IPPubCappingRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
    extends GenericCountRule(params: Parameter, bit: Long, dateFiles: DateFiles,  cappingRuleJobObj: BaseSparkJob, window: String) {

  import cappingRuleJobObj.spark.implicits._

  //workdir
  override lazy val fileName = "/ipPub_" + window + "/"
  //specific columns for ipPub Capping Rule
  override val cols = Array(col("IP"), col("publisher_id"))

  //filter condition for counting df
  def filterCondition(): Column = {
    $"publisher_id" =!= -1
  }

  //counting columns
  def selectCondition(): Array[Column] = {
    val col0 = split(split($"request_headers", "X-eBay-Client-IP: ")(1), """\|""")(0).alias("IP")
    Array(col0, cols(1))
  }

  //add new column if needed
  def withColumnCondition(): Column = {
    when($"channel_action" === "CLICK", split(split($"request_headers", "X-eBay-Client-IP: ")(1), """\|""")(0))
        .otherwise("NA")
  }

  //final join condition
  def joinCondition(df: DataFrame, dfCount: DataFrame): Column = {
    df.col(cols(0).toString()) === dfCount.col(cols(0).toString()) && df.col(cols(1).toString()) === dfCount.col(cols(1).toString())
  }

  override def test(): DataFrame = {

    // filter click only, and publisher_id != -1
    var dfIPPub = dfFilterInJob(filterCondition())

    val head = dfIPPub.take(1)
    if (head.length == 0) {
      var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files).withColumn("capping", lit(0l))
      df
    }
    else {
      val firstRow = head(0)
      val timestamp = dfIPPub.select($"timestamp").first().getLong(0)

      //count by ip and publisher_id in the job
      dfIPPub = dfCountInJob(dfIPPub, selectCondition())

      // reduce the number of counting file to 1, and rename file name to include timestamp
      repartitionAndRename(dfIPPub, timestamp)

      //DataFrame for join
      var df = dfForJoin(cols(0), withColumnCondition())

      //read previous data and add to count path
      val ipCountPath = readCountData(timestamp)

      //count through whole timeWindow and filter those over threshold
      dfIPPub = dfCountAllAndFilter(dfIPPub, ipCountPath)

      //join origin df and counting df
      df = dfJoin(df, dfIPPub, joinCondition(df, dfIPPub), cols(0))
      df
    }
  }
}
