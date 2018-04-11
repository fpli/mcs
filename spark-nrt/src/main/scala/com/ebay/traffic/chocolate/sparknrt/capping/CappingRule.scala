package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.{DataFrame}

/**
  * Created by xiangli4 on 4/8/18.
  */
trait CappingRule {
  def cappingBit: Long

  def cleanBaseDir()

  def test(dateFiles: DateFiles): DataFrame

  def renameBaseTempFiles(dateFiles: DateFiles)
}

object CappingRuleEnum extends Enumeration {
  implicit def getBitValue(value: Value): Long = {
    0x1 << value.id
  }

  val IPCappingRule = Value(0)
  val SnidCappingRUle = Value(1)

}
