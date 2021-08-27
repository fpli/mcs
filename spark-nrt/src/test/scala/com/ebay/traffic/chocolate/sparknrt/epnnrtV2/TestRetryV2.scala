package com.ebay.traffic.chocolate.sparknrt.epnnrtV2


import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.utils.RetryUtil
class TestRetryV2 extends BaseFunSuite{

  def getData(): Option[String] = {
    var now: Long =System.currentTimeMillis()
    try {
      RetryUtil.retry {
        val d: Double = Math.random()
        val current: Long =System.currentTimeMillis()
        println("duration="+(current-now))
        now=current
        if (d < 0.01) {
          Some("success")
        } else {
          println(d)
          throw new Exception
        }
      }
    } catch {
      case e: Exception => {
        Some("fail")
      }
    }
  }

  test("test retry util") {
    assert(getData().get=="success")
  }
}