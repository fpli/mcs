package com.ebay.traffic.chocolate.sparknrt.meta

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/**
  * Created by yliu29 on 3/23/18.
  */
class TestMetadata extends BaseFunSuite {

  test("Test Metadata") {

    val tempDir = createTempPath()
    val metadata = Metadata(tempDir, "EPN", MetadataEnum.dedupe)

    val dateFiles1 = new DateFiles("2018-3-22", Array("hdfs://nn/file1", "hdfs://nn/file2"))
    val dateFiles2 = new DateFiles("2018-3-23", Array("hdfs://nn/file1", "hdfs://nn/file2", "hdfs://nn/file3"))
    val meta: MetaFiles = new MetaFiles(Array(dateFiles1, dateFiles2))

    // test dedupe comp meta
    assert (metadata.readDedupeCompMeta == null)

    metadata.writeDedupeCompMeta(meta)

    val resultDedupeCompMeta = metadata.readDedupeCompMeta

    assert((resultDedupeCompMeta.get("2018-3-22").get)(0) == (dateFiles1.files)(0))

    assert((resultDedupeCompMeta.get("2018-3-23").get)(2) == (dateFiles2.files)(2))

    // test dedupe output meta
    metadata.writeDedupeOutputMeta(meta)
    metadata.writeDedupeOutputMeta(meta)

    val resultDedupeOutputMeta = metadata.readDedupeOutputMeta()
    assert(resultDedupeOutputMeta.length == 2)
    assert((resultDedupeOutputMeta(0)._2.get("2018-3-22").get)(0) == (dateFiles1.files)(0))
    assert((resultDedupeOutputMeta(1)._2.get("2018-3-23").get)(2) == (dateFiles2.files)(2))

  }

}
