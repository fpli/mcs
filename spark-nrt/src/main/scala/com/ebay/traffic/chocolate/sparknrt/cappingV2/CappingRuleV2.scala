package com.ebay.traffic.chocolate.sparknrt.cappingV2

import org.apache.spark.sql.DataFrame
import scala.language.implicitConversions

/**
 * Created by yuhxiao on 1/3/21.
  */
trait CappingRuleV2 {

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
  val SnidCappingRule_L = Value(8)
  val IPBrowserCappingRule_S = Value(9)
  val IPBrowserCappingRule_M = Value(10)
  val IPBrowserCappingRule_L = Value(11)
}
