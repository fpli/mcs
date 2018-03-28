package com.ebay.traffic.chocolate.sparknrt.meta

import com.ebay.traffic.chocolate.spark.BaseFunSuite

/**
  * Created by yliu29 on 3/23/18.
  */
class TestMetadata extends BaseFunSuite {

  test("Test Metadata") {

    val tempDir = createTempPath()
    val metaData = Metadata(tempDir)

    val dateFiles1 = new DateFiles("2018-3-22", Array("hdfs://nn/file1", "hdfs://nn/file2"))
    val dateFiles2 = new DateFiles("2018-3-23", Array("hdfs://nn/file1", "hdfs://nn/file2", "hdfs://nn/file3"))
    val dedupeCompMeta: MetaFiles = new MetaFiles(Array(dateFiles1, dateFiles2))

    metaData.writeDedupeCompMeta(dedupeCompMeta)

    val resultDedupeCompMeta = metaData.readDedupeCompMeta

    assert((resultDedupeCompMeta.get("2018-3-22").get)(0) == (dateFiles1.files)(0))

    assert((resultDedupeCompMeta.get("2018-3-23").get)(2) == (dateFiles2.files)(2))

  }

}
