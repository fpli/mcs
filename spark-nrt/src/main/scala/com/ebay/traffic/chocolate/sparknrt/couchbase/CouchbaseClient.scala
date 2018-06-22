package com.ebay.traffic.chocolate.sparknrt.couchbase

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import org.slf4j.LoggerFactory

/**
  * Created by weibdai on 5/19/18.
  */
object CouchbaseClient {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient private lazy val properties = {
    val properties = new Properties
    properties.load(getClass.getClassLoader.getResourceAsStream("couchbase.properties"))
    properties
  }

  // Use this delegate to override the way for cluster creation.
  @transient var createClusterFunc: () => Cluster = createCluster

  // Open Couchbase bucket with settings in couchbase.properties.
  lazy val cluster: Cluster = createClusterFunc()

  def createCluster(): Cluster = {
    try {
      val nodes = properties.getProperty("chocolate.couchbase.cluster")
      val user = properties.getProperty("chocolate.couchbase.user")
      val password = properties.getProperty("chocolate.couchbase.password")
      val env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(10000).queryTimeout(5000).build()
      CouchbaseCluster.create(env, nodes).authenticate(user, password)
    } catch {
      case e: Exception =>
        logger.error("Couchbase create cluster error.", e)
        throw e
    }
  }

  @transient lazy val reportBucket: Bucket = {
    try {
      val bucketName = properties.getProperty("chocolate.report.couchbase.bucket")
      cluster.openBucket(bucketName, 1200, TimeUnit.SECONDS)
    } catch {
      case e: Exception =>
        logger.error("Couchbase open bucket error.", e)
        throw e
    }
  }

  @transient lazy val dedupeBucket: Bucket = {
    try {
      val bucketName = properties.getProperty("chocolate.dedupe.couchbase.bucket")
      cluster.openBucket(bucketName, 1200, TimeUnit.SECONDS)
    } catch {
      case e: Exception =>
        logger.error("Couchbase open bucket error.", e)
        throw e
    }
  }

/**
  * Insert or append data into Couchbase.
  *
  * @param key     key of data
  * @param mapData value of data organized in Map[String, Any]
  */
def upsertMap(key: String, mapData: Map[String, _]): Unit = {
  try {
    val jsonObject = JsonObject.empty()
    mapData.foreach(data => jsonObject.put(data._1, data._2))

    logger.debug("Couchbase upsert: " + key + " -> " + jsonObject)

    if (!reportBucket.exists(key)) {
      val jsonArray = JsonArray.create().add(jsonObject)
      val root = JsonObject.empty().put("data", jsonArray)
      reportBucket.upsert(JsonDocument.create(key, root))
    } else {
      val jsonArray = reportBucket.get(key).content().getArray("data")
      jsonArray.add(jsonObject)
      val root = JsonObject.empty().put("data", jsonArray)
      reportBucket.replace(JsonDocument.create(key, root))
    }
  } catch {
    case e: Exception => { logger.error("Couchbase upsert error.", e) }
  }
}

  /**
    * Close bucket connection.
    */
  def close(bucket: Bucket): Unit = {
    try {
      bucket.close
    } catch {
      case e: Exception => {
        logger.error("Couchbase bucket close error.", e)
      }
    } finally {
      cluster.disconnect()
    }
  }
}