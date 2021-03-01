package com.ebay.traffic.chocolate.sparknrt.capping_v2.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping_v2.Parameter_v2
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by jialili1 on 5/28/18.
  */
class CGUIDCappingRule(params: Parameter_v2, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
    extends GenericCountRule(params: Parameter_v2, bit: Long, dateFiles: DateFiles,  cappingRuleJobObj: BaseSparkJob, window: String) {

  import cappingRuleJobObj.spark.implicits._

  //workdir
  override lazy val fileName = "/cguid_" + window + "/"
  //specific columns for CGUID Capping Rule
  override val cols = Array(col("CGUID"))

  //filter condition for counting df
  def filterCondition(): Column = {
    $"channel_action" === "CLICK"
  }

  // get CGUID
  def cguid(): Column = {
    $"cguid".alias("CGUID")
  }

  //counting columns
  def selectCondition(): Array[Column] = {
    Array(cguid())
  }

  //add new column if needed
  def withColumnCondition(): Column = {
    when($"channel_action" === "CLICK", cguid()).otherwise("NA")
  }

  //final join condition
  def joinCondition(df: DataFrame, dfCount: DataFrame): Column = {
    df.col(cols(0).toString()) === dfCount.col(cols(0).toString())
  }

  override def test(): DataFrame = {

    //Step 1: Prepare counting data. If this job has no events, return snapshot_id and capping = 0.
    //filter click only
    var dfCGUID = dfFilterInJob(filterCondition())

    //if job has no events, then return df with capping column directly
    val head = dfCGUID.take(1)
    if (head.length == 0) {
      dfNoEvents()
    }
    else {
      val timestamp = dfCGUID.select($"timestamp").first().getLong(0)

      //Step 2: Count by CGUID in this job, then integrate data to 1 file, and add timestamp to file name.
      //count by CGUID and in the job
      dfCGUID = dfLoadCappingInJob(dfCGUID, selectCondition())

      //reduce the number of counting file to 1, and rename file name to include timestamp
      saveCappingInJob(dfCGUID, timestamp)

      //Step 3: Read a new df for join purpose, just select CGUID and snapshot_id, and read previous data for counting purpose.
      //df for join
      val selectCols: Array[Column] = $"snapshot_id" +: cols
      var df = dfForJoin(cols(0), withColumnCondition(), selectCols)

      //read previous data and add to count path
      val cguidCountPath = getCappingDataPath(timestamp)

      //Step 4: Count all data, including previous data and data in this job, then join the result with the new df, return only snapshot_id and capping.
      //count through whole timeWindow and filter those over threshold
      dfCGUID = dfCappingInJob(null, cguidCountPath)

      //join origin df and counting df
      df = dfJoin(df, dfCGUID, joinCondition(df, dfCGUID))
      df
    }
  }
}
