package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.util.Properties

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.epnnrt.BullseyeUtils.{getClass, properties}
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}

/**
 * @Auther YangYang
 */
class TestSecretClient() extends BaseFunSuite {
  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  test("testFetchClientSecret") {
    assert(SecretClient.getSecretByClientId(properties.getProperty("epnnrt.clientId")) != "")
  }
}
