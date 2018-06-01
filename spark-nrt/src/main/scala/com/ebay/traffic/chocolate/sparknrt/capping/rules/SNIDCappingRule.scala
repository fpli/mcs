package com.ebay.traffic.chocolate.sparknrt.capping.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.Parameter
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

/**
  * Created by xiangli4 on 5/31/18.
  */
// Snid capping rule is a special rule that it has 2 capping bits, short and long
// When extending GenericRule, the bit is set as the long bit, but it won't affect. This parameter is never used in this rule
class SNIDCappingRule(params: Parameter, bitLong: Long, bitShort: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
  extends GenericRule(params: Parameter, bitLong, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window) with Serializable {

  import cappingRuleJobObj.spark.implicits._

  // workdir
  override lazy val fileName = "/snid/"
  override lazy val ruleType = "snid_timeWindow_long"
  lazy val ruleTypeLong = "snid_timeWindow_long"
  lazy val ruleTypeShort = "snid_timeWindow_short"
  // snid use long time window to get previous data
  override lazy val timeWindow = properties.getProperty(ruleTypeLong).toLong
  // time window short will be used in executor, make it lazy
  lazy val timeWindowShort = properties.getProperty(ruleTypeShort).toLong

  @transient override val cols: Array[Column] = Array(col("snapshot_id"), col("snid"),
    col("timestamp"), col("channel_action"))

  // filter condition for counting df
  def filterImpressionCondition(): Column = {
    $"publisher_id" =!= -1 and $"channel_action" === "IMPRESSION"
  }

  //filter condition for counting df
  def filterClickCondition(): Column = {
    $"publisher_id" =!= -1 and $"channel_action" === "CLICK"
  }

  // select columns
  def selectCondition(): Array[Column] = {
    cols
  }

  // final join condition
  def joinCondition(df: DataFrame, dfPrev: DataFrame): Column = {
    $"snapshot_id" === $"snapshot_id_1"
  }

  import cappingRuleJobObj.spark.implicits._

  override def dfLoadCappingInJob(dfCapping: DataFrame, selectCols: Array[Column]): DataFrame = {
    dfCapping.select(selectCols: _*)
  }

  // snid capping logic
  // key is snid
  // events is impressions & clicks sharing the same snid
  def snidCapping(key: String, events: Iterable[Row]): Iterator[Tuple2[Long, Long]] = {

    var eventsIter = events.iterator
    var existImpression = false
    var impressionTimestamp = 0l
    while (eventsIter.hasNext) {
      val event = eventsIter.next()
      if (event.getString(event.fieldIndex("channel_action")).equals("IMPRESSION")) {
        existImpression = true
        impressionTimestamp = event.getLong(event.fieldIndex("timestamp"))
      }
    }

    var cappingEvents = new ListBuffer[Tuple2[Long, Long]]
    eventsIter = events.iterator

    while (eventsIter.hasNext) {
      val event = eventsIter.next()
      if (event.getString(event.fieldIndex("channel_action")).equals("CLICK")) {
        if (!existImpression) {
          cappingEvents += Tuple2(event.getLong(event.fieldIndex("snapshot_id")), bitLong)
        }
        else {
          val clickTimestamp = event.getLong(event.fieldIndex("timestamp"))
          if (clickTimestamp - impressionTimestamp < timeWindowShort) {
            cappingEvents += Tuple2(event.getLong(event.fieldIndex("snapshot_id")), bitShort)
          }
          else {
            cappingEvents += Tuple2(event.getLong(event.fieldIndex("snapshot_id")), 0l)
          }
        }
      }
      else {
        cappingEvents += Tuple2(event.getLong(event.fieldIndex("snapshot_id")), 0l)
      }
    }
    cappingEvents.iterator
  }

  override def dfCappingInJob(dfJoin: DataFrame, cappingPath: List[String]): DataFrame = {

    // read impression data with snid
    val dfRight = cappingRuleJobObj.readFilesAsDFEx(cappingPath.toArray)
    dfRight.show()
    // union current batch data. we don't drop duplicates impression in current batch for better performance
    // use rdd instead of dataframe as in this scenario, rdd is easy to apply single record processing
    val snidRDD = dfJoin.union(dfRight).rdd

    // group records by snid
    val groupedRDD = snidRDD.groupBy(row => row.getString(row.fieldIndex("snid")))

    // snid logic
    val cappingRDD = groupedRDD.flatMap {
      snid2Records => {
        snidCapping(snid2Records._1, snid2Records._2)
      }
    }
    // convert back to dataframe with schema
    cappingRDD.toDF("snapshot_id_1", "capping")
  }

  override def test(): DataFrame = {

    // Step 1: Prepare map data. If this job has no events, return snapshot_id and capping = 0.
    // filter impression only, and publisher_id != -1
    var dfSnid = dfFilterInJob(filterImpressionCondition())
    var dfClick = dfFilterInJob(filterClickCondition())
    val headImpression = dfSnid.take(1)
    val headClick = dfClick.take(1)

    // if this batch has impression, load capping data and save in hdfs
    if (headImpression.length != 0) {
      val firstRow = headImpression(0)
      val timestamp = dfSnid.select($"timestamp").first().getLong(0)

      // Step 2: Select all the impression snid, then integrate data to 1 file, and add timestamp to file name.
      // get impression snids in the job
      dfSnid = dfLoadCappingInJob(dfSnid, selectCondition())

      //reduce the number of counting file to 1, and rename file name to include timestamp
      saveCappingInJob(dfSnid, timestamp)
    }

    // if this batch has click
    if (headClick.length != 0) {
      val timestamp = dfClick.select($"timestamp").first.getLong(0)
      // Step 3: Read a new df for join purpose, just select snid and snapshot_id
      // df for join
      val selectCols: Array[Column] = $"snapshot_id" +: cols
      var df = dfForJoin(null, null, cols)

      // read previous data and add to count path
      val cappingPath = getCappingDataPath(timestamp)

      // Step 4: Get all data, including previous data and data in this job, then join the result with the new df, return only snapshot_id and capping.
      // count through whole timeWindow and filter those over threshold
      dfSnid = dfCappingInJob(df, cappingPath)

      // Step 5: Join back current batch
      // join origin df and counting df
      dfJoin(df, dfSnid, joinCondition(df, dfSnid))
    }
    else {
      dfNoEvents()
    }
  }
}
