package com.ebay.traffic.chocolate.sparknrt.couchbase

import java.util.Properties

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.datastructures.MutationOptionBuilder
import com.ebay.dukes.CacheClient
import com.ebay.dukes.base.BaseDelegatingCacheClient
import com.ebay.dukes.couchbase2.Couchbase2CacheClient
import org.slf4j.LoggerFactory

object CorpCouchbaseClient {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient private lazy val properties = {
    val properties = new Properties
    properties.load(getClass.getClassLoader.getResourceAsStream("couchbase.properties"))
    properties
  }

  @transient private lazy val dataSource: String = properties.getProperty("chocolate.corp.couchbase.dataSource")

  @transient private lazy val factory =
    com.ebay.dukes.builder.GenericCacheFactoryBuilder.newBuilder()
      .cache(dataSource)
      .identityFileDirectoryLocation(getClass.getResource("/identities/").getPath)
      .dbEnv(properties.getProperty("chocolate.corp.couchbase.dbEnv"))
      .deploymentSlot(properties.getProperty("chocolate.corp.couchbase.deploymentSlot"))
      .dnsRegion(properties.getProperty("chocolate.corp.couchbase.dnsRegion"))
      .pool(properties.getProperty("chocolate.corp.couchbase.pool"))
      .poolType(properties.getProperty("chocolate.corp.couchbase.poolType"))
      .appName(properties.getProperty("chocolate.corp.couchbase.appName"))
      .build()

  @transient var getBucketFunc: () => (Option[CacheClient], Bucket) = getBucket

  /**
    * Insert or append data into Corp Couchbase.
    *
    * @param key     key of data
    * @param mapData value of data organized in Map[String, Any]
    */
  def upsertMap(key: String, mapData: Map[String, _]): Unit = {
    try {
      val jsonObject = JsonObject.empty()
      mapData.foreach(data => jsonObject.put(data._1, data._2))

      logger.debug("Corp Couchbase upsert: " + key + " -> " + jsonObject)
      val (cacheClient, bucket) = getBucketFunc()
      bucket.listAppend(key, jsonObject, MutationOptionBuilder.builder().createDocument(true))
      returnClient(cacheClient)
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase upsert error.", e)
      }
    }
  }

  /**
    * get bucket
    */
  def getBucket(): (Option[CacheClient], Bucket) = {
    val cacheClient: CacheClient = factory.getClient(dataSource)
    val baseClient: BaseDelegatingCacheClient = cacheClient.asInstanceOf[BaseDelegatingCacheClient]
    val cbCacheClient: Couchbase2CacheClient = baseClient.getCacheClient.asInstanceOf[Couchbase2CacheClient]
    (Option(cacheClient), cbCacheClient.getCouchbaseClient)
  }

  /**
    * return cacheClient to factory
    */
  def returnClient(cacheClient: Option[CacheClient]): Unit = {
    try {
      if (cacheClient.isDefined) {
        factory.returnClient(cacheClient.get)
      }
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  /**
    * Close corp couchbase connection.
    */
  def close(): Unit = {
    try {
      factory.shutdown()
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase bucket close error.", e)
      }
    }
  }
}