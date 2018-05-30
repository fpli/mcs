package com.ebay.traffic.chocolate.sparknrt.capping

import org.apache.spark.sql.DataFrame

/**
  * Created by xiangli4 on 4/8/18.
  */
trait CappingRule {

  // date col name
  def DATE_COL: String

  // capping rule result bit value
  def cappingBit: Long

  // clean jobs before test
  def preTest()

  // test logic
  def test(): DataFrame

  // clean jobs after test
  def postTest()
}

// Enum of CappingRule name and bitmap value
object CappingRuleEnum extends Enumeration {
  implicit def getBitValue(value: Value): Long = {
    0x1 << value.id
  }

  val IPCappingRule = Value(0)
  val IPPubCappingRule_S = Value(1)
  val IPPubCappingRule_L = Value(2)
  val CGUIDPubCappingRule_S = Value(3)
  val CGUIDPubCappingRule_L = Value(4)
  val CGUIDCappingRule_S = Value(5)
  val CGUIDCappingRule_L = Value(6)
  val SnidCappingRule_S = Value(7)
  val snidCappingRule_L = Value(8)
}
