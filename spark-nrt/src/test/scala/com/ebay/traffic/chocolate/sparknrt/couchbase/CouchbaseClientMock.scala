package com.ebay.traffic.chocolate.sparknrt.couchbase

import java.util

import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.{Cluster, CouchbaseCluster}
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
  val testBucketName = "default"
  var cluster = null

  private def createCouchbaseMock(password: String): Unit = {
    if(couchbaseMock == null) {
      val bucketConfiguration = new BucketConfiguration()
      bucketConfiguration.numNodes = 1
      bucketConfiguration.numReplicas = 1
      bucketConfiguration.numVBuckets = 1024
      bucketConfiguration.name = testBucketName
      bucketConfiguration.`type` = com.couchbase.mock.Bucket.BucketType.COUCHBASE
      bucketConfiguration.password = password


      val configList = new util.ArrayList[BucketConfiguration]
      configList.add(bucketConfiguration)

      couchbaseMock = new CouchbaseMock(0, configList)
      couchbaseMock.start()
      couchbaseMock.waitForStartup()
    }
  }

  private def initMock(): (Int, Int) = {
    val httpPort = couchbaseMock.getHttpPort
    val builder = new URIBuilder
    builder.setScheme("http").setHost("localhost").setPort(httpPort).setPath("mock/get_mcports")
      .setParameter("bucket", testBucketName)
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
    createCouchbaseMock("")
  }

  def connect(): Cluster = {
    if(cluster == null) {
      val handle = initMock()
      CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder
        .bootstrapCarrierDirectPort(handle._1)
        .bootstrapHttpDirectPort(handle._2)
        .build(), "couchbase://127.0.0.1")
    }
    else {
      cluster
    }
  }

  def closeCouchbaseMock(): Unit = {
    if (couchbaseMock != null) couchbaseMock.stop
  }
}
