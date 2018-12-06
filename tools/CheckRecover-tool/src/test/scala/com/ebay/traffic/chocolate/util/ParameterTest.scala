package com.ebay.traffic.chocolate.util

import com.ebay.traffic.chocolate.spark.BaseFunSuite

class ParameterTest extends BaseFunSuite {

  test("test parameter") {
    val arr = Array("--appName",
      "checkJob",
      "--mode",
      "local",
      "--countDataDir",
      "countFileDir",
      "--ts",
      "1543477214789",
      "--taskFile",
      "testCheck",
      "--elasticsearchUrl",
      "http://10.148.181.34:9200");
    val parameter = Parameter(arr);

    assert(parameter.appName == "checkJob");
    assert(parameter.mode == "local");
    assert(parameter.countDataDir == "countFileDir");
    assert(parameter.ts == "1543477214789");
    assert(parameter.taskFile == "testCheck");
    assert(parameter.elasticsearchUrl == "http://10.148.181.34:9200");
  }

}
