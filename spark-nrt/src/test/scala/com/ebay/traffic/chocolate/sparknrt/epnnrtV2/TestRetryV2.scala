package com.ebay.traffic.chocolate.sparknrt.epnnrtV2


import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.utils.RetryUtil

class TestRetryV2 extends BaseFunSuite {
  def getData(num: Int): Option[Int] = {
    var count = 0
    try {
      RetryUtil.retry {
        val d: Double = Math.random()
        if (d < num) {
          count=count+1
          throw new Exception
        } else {
          Some(count)
        }
      }
    } catch {
      case e: Exception => {
        Some(count)
      }
    }
  }

  test("test retry util fail") {
    assert(getData(2).get == 5)
  }
  test("test retry util success") {
    assert(getData(-1).get == 0)
  }
}
