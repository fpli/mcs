package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import org.apache.spark.sql.DataFrame

/**
  * Created by xiangli4 on 4/8/18.
  */
trait CappingRule {

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
  val SnidCappingRule = Value(1)

}
