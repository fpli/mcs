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
    $"publisher_id" =!= -1 and $"channel_action" === "CLICK"
  }

  //parse IP from request_headers
  def parseIP(): Column = {
    split(split($"request_headers", "X-eBay-Client-IP: ")(1), """\|""")(0).alias("IP")
  }

  //counting columns
  def selectCondition(): Array[Column] = {
    Array(parseIP(), cols(1))
  }

  //add new column if needed
  def withColumnCondition(): Column = {
    when($"channel_action" === "CLICK", parseIP()).otherwise("NA")
  }

  //final join condition
  def joinCondition(df: DataFrame, dfCount: DataFrame): Column = {
    df.col(cols(0).toString()) === dfCount.col(cols(0).toString()) && df.col(cols(1).toString()) === dfCount.col(cols(1).toString())
  }

  override def test(): DataFrame = {

    //Step 1: Prepare counting data. If this job has no events, return snapshot_id and capping = 0.
    //filter click only, and publisher_id != -1
    var dfIPPub = dfFilterInJob(filterCondition())

    //if job has no events, then return df with capping column directly
    val head = dfIPPub.take(1)
    if (head.length == 0) {
      dfNoEvents()
    }
    else {
      val firstRow = head(0)
      val timestamp = dfIPPub.select($"timestamp").first().getLong(0)

      //Step 2: Count by ip and publisher_id in this job, then integrate data to 1 file, and add timestamp to file name.
      //count by ip and publisher_id in the job
      dfIPPub = dfLoadCappingInJob(dfIPPub, selectCondition())

      //reduce the number of counting file to 1, and rename file name to include timestamp
      saveCappingInJob(dfIPPub, timestamp)

      //Step 3: Read a new df for join purpose, just select IP, publisher_id and snapshot_id, and read previous data for counting purpose.
      //df for join
      val selectCols: Array[Column] = $"snapshot_id" +: cols
      var df = dfForJoin(cols(0), withColumnCondition(), selectCols)

      //read previous data and add to count path
      val ipCountPath = getCappingDataPath(timestamp)

      //Step 4: Count all data, including previous data and data in this job, then join the result with the new df, return only snapshot_id and capping.
      //count through whole timeWindow and filter those over threshold
      dfIPPub = dfCappingInJob(null, null, null, ipCountPath)

      //join origin df and counting df
      df = dfJoin(df, dfIPPub, joinCondition(df, dfIPPub))
      df
    }
  }
}
