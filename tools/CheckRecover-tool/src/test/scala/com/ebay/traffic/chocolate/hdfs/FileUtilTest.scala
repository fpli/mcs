package com.ebay.traffic.chocolate.hdfs

import com.ebay.traffic.chocolate.spark.BaseFunSuite

class FileUtilTest extends BaseFunSuite{

  test("test isExistFile"){
    val isExist = FileUtil.isExistFile(getTestResourcePath("testCheck"), "")
    assert(isExist == true)
  }

}
