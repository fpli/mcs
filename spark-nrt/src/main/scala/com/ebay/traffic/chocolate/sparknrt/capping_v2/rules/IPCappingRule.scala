package com.ebay.traffic.chocolate.sparknrt.capping_v2.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping_v2.Parameter_v2
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by xiangli4 on 4/8/18.
  */
class IPCappingRule(params: Parameter_v2, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
  extends GenericCountRule(params: Parameter_v2, bit: Long, dateFiles: DateFiles,  cappingRuleJobObj: BaseSparkJob, window: String) {

  import cappingRuleJobObj.spark.implicits._

  //workdir
  override lazy val fileName = "/ip_" + window + "/"
  //specific columns for IP Capping Rule
  override val cols = Array(col("IP"))

  //filter condition for counting df
  def filterCondition(): Column = {
    $"channel_action" === "CLICK"
  }

  //get IP
  def ip(): Column = {
    $"remote_ip".alias("IP")
  }

  //counting columns
  def selectCondition(): Array[Column] = {
    Array(ip())
  }

  //add new column if needed
  def withColumnCondition(): Column = {
    when($"channel_action" === "CLICK", ip()).otherwise("NA")
  }

  //final join condition
  def joinCondition(df: DataFrame, dfCount: DataFrame): Column = {
    df.col(cols(0).toString()) === dfCount.col(cols(0).toString())
  }

  override def test(): DataFrame = {

    //Step 1: Prepare counting data. If this job has no events, return snapshot_id and capping = 0.
    //filter click only
    var dfIP = dfFilterInJob(filterCondition())

    //if job has no events, then return df with capping column directly
    val head = dfIP.take(1)
    if (head.length == 0) {
      dfNoEvents()
    }
    else {
      val timestamp = dfIP.select($"timestamp").first().getLong(0)

      //Step 2: Count by IP in this job, then integrate data to 1 file, and add timestamp to file name.
      //count by IP and in the job
      dfIP = dfLoadCappingInJob(dfIP, selectCondition())

      //reduce the number of counting file to 1, and rename file name to include timestamp
      saveCappingInJob(dfIP, timestamp)

      //Step 3: Read a new df for join purpose, just select IP and snapshot_id, and read previous data for counting purpose.
      //df for join
      val selectCols: Array[Column] = $"snapshot_id" +: cols
      var df = dfForJoin(cols(0), withColumnCondition(), selectCols)

      //read previous data and add to count path
      val ipCountPath = getCappingDataPath(timestamp)

      //Step 4: Count all data, including previous data and data in this job, then join the result with the new df, return only snapshot_id and capping.
      //count through whole timeWindow and filter those over threshold
      dfIP = dfCappingInJob(null, ipCountPath)

      //join origin df and counting df
      df = dfJoin(df, dfIP, joinCondition(df, dfIP))
      df
    }
  }
}
