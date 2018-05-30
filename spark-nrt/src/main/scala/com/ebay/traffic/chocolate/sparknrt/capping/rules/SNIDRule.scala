package com.ebay.traffic.chocolate.sparknrt.capping.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.Parameter
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Created by xiangli4 on 5/29/18.
  */
class SNIDRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
  extends GenericMapRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String) {

  import cappingRuleJobObj.spark.implicits._

  //workdir
  override lazy val fileName = "/snid_" + window + "/"

  override lazy val ruleType = "snid_timeWindow_" + window
  override lazy val timeWindow = properties.getProperty(ruleType).toLong

  override val cols: Array[Column] = Array(col("snid"), col("timestamp"), col("channel_action"))

  lazy val rightSelectCol: Array[Column] = Array(col(cols(0).toString() + "_1"), col(cols(1).toString() + "_1"), col(cols(2).toString() + "_1"))

  // filter impression condition
  def filterImpressionCondition(): Column = {
    $"publisher_id" =!= -1 and $"channel_action" === "IMPRESSION"
  }

  // filter click condition
  def filterClickCondition(): Column = {
    $"publisher_id" =!= -1 and $"channel_action" === "CLICK"
  }

  //select columns
  def selectCondition(): Array[Column] = {
    cols
  }

  // final join condition
  def joinCondition(df: DataFrame, dfPrev: DataFrame): Column = {
    $"snapshot_id" === $"snapshot_id_1"
  }

  // when condition
  def whenCondition(): Column = {
    if (window.equalsIgnoreCase("long")) {
      $"channel_action" === "CLICK" and rightSelectCol(0).isNull
    }
    else {
      $"channel_action" === "CLICK" and rightSelectCol(0).isNotNull and (cols(1) - rightSelectCol(1) < timeWindow)
    }
  }

  override def test(): DataFrame = {

    // Step 1: Prepare map data. If this job has no events, return snapshot_id and capping = 0.
    // filter impression only, and publisher_id != -1
    var dfImpressionSNID = dfFilterInJob(filterImpressionCondition())

    var dfClick = dfFilterInJob(filterClickCondition())

    val headImpression = dfImpressionSNID.take(1)
    val headClick = dfClick.take(1)

    // if job has no impression, don't do save map file
    if (headImpression.length != 0) {
      val timestamp = dfImpressionSNID.select($"timestamp").first().getLong(0)
      // Step 2: Select all the impression snid, then integrate data to 1 file, and add timestamp to file name.
      // get impression snids in the job
      dfImpressionSNID = dfLoadCappingInJob(dfImpressionSNID, selectCondition())
          .withColumnRenamed(cols(0).toString(), rightSelectCol(0).toString())
          .withColumnRenamed(cols(1).toString(), rightSelectCol(1).toString())
          .withColumnRenamed(cols(2).toString(), rightSelectCol(2).toString())

      // reduce the number of map file to 1, and rename file name to include timestamp
      saveCappingInJob(dfImpressionSNID, timestamp)
    }

    // if job has no click, return no events
    if (headClick.length != 0) {
      val timestamp = dfClick.select($"timestamp").first().getLong(0)

      // Step 3: Read a new df for join purpose, just select snid and snapshot_id
      // df for join
      val selectCols: Array[Column] = $"snapshot_id" +: cols
      var df = dfForJoin(null, null, selectCols)

      // read previous data and add to map path
      val snidMapPath = getCappingDataPath(timestamp)

      // Step 4: Get all data, including previous data and data in this job, then join the result with the new df, return only snapshot_id and capping.
      val dfRight = dfCappingInJob(df, $"snid" === $"snid_1", whenCondition, snidMapPath)
      df = dfJoin(df, dfRight, joinCondition(df, dfRight))
      df
    }
    else {
      dfNoEvents()
    }
  }

}
