package com.ebay.traffic.chocolate.sparknrt.reporting

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import org.slf4j.LoggerFactory

/**
  * Created by weibdai on 5/19/18.
  */
class CouchbaseClient {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  var cluster: Cluster = null
  var bucket: Bucket = null

  def init(nodes: String, bucketName: String, user: String, password: String): Unit = {
    val env = DefaultCouchbaseEnvironment.builder()
      .connectTimeout(10000).queryTimeout(5000).build()

    cluster = CouchbaseCluster.create(env, nodes)

    try {
      cluster.authenticate(user, password)
      bucket = cluster.openBucket(bucketName, 1200, TimeUnit.SECONDS)
    } catch {
      case e: Exception => {
        logger.error("Couchbase init error.", e)
        throw e
      }
    }
  }

  def init(cluster: Cluster, bucket: Bucket): Unit = {
    this.cluster = cluster
    this.bucket = bucket
  }

  def upsert(key: String, mapData: Map[String, _]): Unit = {
    try {
      val jsonObject = JsonObject.empty()
      mapData.foreach(data => jsonObject.put(data._1, data._2))

      logger.debug("Couchbase upsert: " + key + " -> " + jsonObject)

      if (!bucket.exists(key)) {
        val jsonArray = JsonArray.create().add(jsonObject)
        val root = JsonObject.empty().put("data", jsonArray)
        bucket.upsert(JsonDocument.create(key, root))
      } else {
        val jsonArray = bucket.get(key).content().getArray("data")
        jsonArray.add(jsonObject)
        val root = JsonObject.empty().put("data", jsonArray)
        bucket.replace(JsonDocument.create(key, root))
      }
    } catch {
      case e: Exception => { logger.error("Couchbase upsert error.", e) }
    }
  }

  def close(): Unit = {
    bucket.close
    cluster.disconnect
  }
}
