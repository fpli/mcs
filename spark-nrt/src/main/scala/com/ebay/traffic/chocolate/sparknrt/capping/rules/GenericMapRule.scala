package com.ebay.traffic.chocolate.sparknrt.capping.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.Parameter
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{lit, sum, when}

/**
  * Created by xiangli4 on 5/29/18.
  * GenericMapRule is used to map valid click by looking back previous impression on some columns
  */
abstract class GenericMapRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
  extends GenericRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String) {

  override def dfLoadCappingInJob(dfCount: DataFrame, selectCols: Array[Column]): DataFrame = {
    dfSelectionInJob(dfCount, selectCols)
  }

  override def dfCappingInJob(dfJoin: DataFrame, joinCol: Column, whenCol: Column, cappingPath: List[String]): DataFrame = {
    val dfCapping = cappingRuleJobObj.readFilesAsDFEx(cappingPath.toArray)
      .distinct()
    dfJoin.join(dfCapping, joinCol, "left_outer")
      .withColumn("capping", when(whenCol, cappingBit).otherwise(lit(0)))
      .withColumnRenamed("snapshot_id", "snapshot_id_1")
      .select("snapshot_id_1", "capping")
  }
}
