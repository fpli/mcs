package com.ebay.traffic.chocolate.sparknrt.epnnrtV2

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.utils.RetryUtil

import scala.collection.immutable.HashMap

class TesSliding extends BaseFunSuite {
  def toMap(list: Array[String]): HashMap[String, String] = {
    var res = new HashMap[String, String]
    list.foreach(item => {
      res = res + (item -> item)
    })
    res
  }

  test("test sliding") {
    val stringArray = Array("This", "is", "a", "string", "array")
    val list = stringArray.sliding(2, 2).toList
    val expect = List(Array("This", "is"), Array("a", "string"), Array("array"))
    assert(list.length === expect.length)
    for (i <- list.indices) {
      assert(list(i).sameElements(expect(i)) === true)
    }
    val maps = list.map(l => toMap(l))
    assert(maps === List(Map("This" -> "This", "is" -> "is"), Map("a" -> "a", "string" -> "string"), Map("array" -> "array")))
    assert(maps.reduce(_ ++ _) === Map("This" -> "This", "is" -> "is", "a" -> "a", "string" -> "string", "array" -> "array"))
  }
}
