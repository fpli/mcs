package com.ebay.traffic.chocolate.util

import java.util.Date

import com.ebay.traffic.chocolate.spark.BaseFunSuite

class XMLUtilTest extends BaseFunSuite {

  test("Test reading the xml file") {
    val file = getTestResourcePath("tasks.xml")
    val parameter = new Parameter("a", "b", "c", "1543477214789", "task.xml", "f")
    val tasks = XMLUtil.readFile(file, parameter)
    for (task <- tasks) {
      assert(task.jobName == "test-checkJob")
    }
  }

  test("verify the timestamp") {
    val real = XMLUtil.getVerifiedTime(1543477214789l)
    assert(real == 1543477200000l)
  }

  test("test the getInputDir") {
    val real = XMLUtil.getInputDir("/test/date=", 1543477200000l, 0)
    assert(real == "/test/date=2018-11-29")
  }

  test("test the getInputDir when diffTime is not 0") {
    val real = XMLUtil.getInputDir("/test/date=", 1546120800000l, 46)
    assert(real == "/test/date=2018-12-28")
  }

  test("test the getDataCountDir") {
    val parameter = new Parameter("test", "b", "/countDataDir", "1543477214789", "task.xml", "f")
    val real = XMLUtil.getDataCountDir("test", parameter)
    assert(real == "/countDataDir/test")
  }


}
