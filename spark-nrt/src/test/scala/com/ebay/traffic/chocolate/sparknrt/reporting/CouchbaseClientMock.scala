package com.ebay.traffic.chocolate.sparknrt.reporting

import java.util
import java.util.concurrent.TimeUnit

import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import com.couchbase.mock.{BucketConfiguration, CouchbaseMock, JsonUtils}
import com.google.gson.JsonObject
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

/**
  * Created by weibdai on 5/19/18.
  */
object CouchbaseClientMock {

  var couchbaseMock: CouchbaseMock = null

  //var httpPort: Int = 0
  //var carrierPort: Int = 0

  private def createCouchbaseMock(name: String, password: String): Unit = {
    val bucketConfiguration = new BucketConfiguration()
    bucketConfiguration.numNodes = 1
    bucketConfiguration.numReplicas = 1
    bucketConfiguration.numVBuckets = 1024
    bucketConfiguration.name = name
    bucketConfiguration.`type` = com.couchbase.mock.Bucket.BucketType.COUCHBASE
    bucketConfiguration.password = password

    val configList = new util.ArrayList[BucketConfiguration]
    configList.add(bucketConfiguration)

    couchbaseMock = new CouchbaseMock(0, configList)
    couchbaseMock.start()
    couchbaseMock.waitForStartup()
  }

  private def initMock(bucket: String): (Int, Int) = {
    val httpPort = couchbaseMock.getHttpPort
    val builder = new URIBuilder
    builder.setScheme("http").setHost("localhost").setPort(httpPort).setPath("mock/get_mcports")
      .setParameter("bucket", bucket)
    val request = new HttpGet(builder.build)
    val client = HttpClientBuilder.create.build
    val response = client.execute(request)
    val status = response.getStatusLine.getStatusCode
    if (status < 200 || status > 300) throw new ClientProtocolException("Unexpected response status: " + status)
    val rawBody = EntityUtils.toString(response.getEntity)
    val respObject = JsonUtils.GSON.fromJson(rawBody, classOf[JsonObject])
    val portsArray = respObject.getAsJsonArray("payload")
    val carrierPort = portsArray.get(0).getAsInt
    (carrierPort, httpPort)
  }

  def startCouchbaseMock(): Unit = {
    createCouchbaseMock("default", "")

  }

  def connect(): (Cluster, Bucket) = {
    val handle = initMock("default")
    val cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder
      .bootstrapCarrierDirectPort(handle._1)
      .bootstrapHttpDirectPort(handle._2)
      .build(), "couchbase://127.0.0.1")
    val bucket = cluster.openBucket("default", 1200, TimeUnit.SECONDS)
    (cluster, bucket)
  }

  def closeCouchbaseMock(): Unit ={
    if (couchbaseMock != null) couchbaseMock.stop
  }
}