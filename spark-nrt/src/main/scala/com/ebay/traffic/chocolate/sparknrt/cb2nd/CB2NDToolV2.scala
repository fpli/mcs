package com.ebay.traffic.chocolate.sparknrt.cb2nd

import java.util
import java.util.{LinkedList, List}

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.view.ViewRow
import com.ebay.dap.epic.monster.migrate.tool.auth.MonstorEnvEnum
import com.ebay.dap.epic.monster.migrate.tool.dao.{MonstorClientUtil, OrderByColumnDefine, OrderByEnum}
import com.ebay.dap.epic.monster.migrate.tool.util.PojoConvertUtil
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import com.google.gson.Gson
import org.apache.commons.lang.StringUtils
import org.joda.time.DateTime
import org.monstor.client.RequestFactory
import org.monstor.client.operations.common.{Document, DocumentFactory, Filter, FilterFactory, Key, KeyFactory, MonstorException}
import org.monstor.client.operations.mutate.{BatchMutateRequest, BatchMutateResponse}
import org.monstor.client.operations.query.{QueryRequest, QueryResponse}
import scalaj.http.Http

object CB2NDToolV2 {
  def main(args: Array[String]) = {
    val job = new CB2NDToolV2()
    job.run()
  }
}

class CB2NDToolV2() {
  def run() = {
    print(Integer.MAX_VALUE)
    val secretEndPoint: String = "https://trackingproxy.vip.qa.ebay.com/tracking/proxy/v1/fidelius/secrets?path=trackingproxy/rotation/secret&secret=Y2JlYjc2YWEtOWU1MC00Y2U1LWJkOTctZjNiNDIyYmUwYmRk"
    var secret = ""
    try {
      var count = 0
      while (StringUtils.isBlank(secret) && count < 3) {
        secret = Http(secretEndPoint).method("GET").timeout(2000, 2000)
          .asString.body
        Thread.sleep(2000)
        count += 1
      }
    } catch {
      case _: Exception =>
    }
    if (StringUtils.isBlank(secret))
      throw new Exception("can't get secret")
    println("secret=" + secret)
    MonstorClientUtil.init("urn:ebay-marketplace-consumerid:181ad791-7ef4-4c84-96fe-691fd7aca187", secret, MonstorEnvEnum.STAGING)
    val monstorUtil = new MonstorClientUtil[RotationInfo]("Chocorotation", 100, 20000)
    val gson = new Gson
    import scala.collection.JavaConversions._
    val views = new util.ArrayList[JsonDocument]
    val r = new RotationInfo
    r.setLast_update_time(System.currentTimeMillis)
    r.setCampaign_id(111111L)
    r.setCampaign_name("123123")
    r.setChannel_id(1111)
    r.setCreate_user("1233")
    r.setRotation_name("ssddsds")
    r.setRotation_string("sdsdsd")
    val mapper: ObjectMapper = new ObjectMapper()
    val m = new util.HashMap[String, String]
    m.put("any", "any")
    r.setRotation_tag(m)
    for (i <- 0 to 50000) {
      r.setRotation_string(i + "")
      views.add(JsonDocument.create(i + "", 0, JsonObject.fromJson(gson.toJson(r))))
    }
    val filter: Filter = FilterFactory.createFilter("last_update_time", Filter.Condition.LESS_THAN_OR_EQUAL, System.currentTimeMillis())
      .and(FilterFactory.createFilter("last_update_time", Filter.Condition.GREATER_THAN_OR_EQUAL, System.currentTimeMillis() - 1000 * 60 * 60 * 5))
    val infoList: util.List[RotationInfo] = new util.ArrayList[RotationInfo]
    //infoList.addAll(monstorUtil.query("rotation", filter, classOf[RotationInfo]))
    val queryStart: Long = System.currentTimeMillis()
    //val keys: util.List[Key] = monstorUtil.listKeys("rotation")
    println("query time spend=" + (System.currentTimeMillis() - queryStart) / 1000)
    val start: Long = System.currentTimeMillis()
    val list: util.List[util.List[JsonDocument]] = Lists.partition(views, 50)
    var failRequests: util.Map[Key, Document] = new util.HashMap[Key, Document]
    for (i <- 0 until list.size()) {
      println("batch=" + i)
      val request: BatchMutateRequest = RequestFactory.of("Chocorotation").startBatchMutation().setTimeout(100000)
      for (j <- 0 until list.get(i).size()) {
        val document: JsonDocument = list.get(i).get(j)
        val rotationInfo: RotationInfo = gson.fromJson(document.content.toString, classOf[RotationInfo])
        val pojoConvertUtil: PojoConvertUtil[RotationInfo] = new PojoConvertUtil[RotationInfo]
        val d: Document = pojoConvertUtil.convertPojoToDocument(rotationInfo)
        val key: Key = KeyFactory.create(rotationInfo.getRotation_string).add("rotation", rotationInfo.getRotation_string)
        request.addForUpsert(key, d)
      }
      val response: BatchMutateResponse = request.execute()
      val total: util.Map[Key, Document] = request.getAllForUpserts
      val iterator: Iterator[Key] = response.getInsertResponse.getErrors.keySet().iterator
      while (iterator.hasNext) {
        val k: Key = iterator.next()
        failRequests.put(k, total.get(k))
      }
      //Thread.sleep(2000)
      println("average batch time=" + ((System.currentTimeMillis() - start) / 1000) / (i + 1))
    }
    println("fail size=" + failRequests.size())
    println("view size=" + views.size())
    val l: Long = System.currentTimeMillis()
    while (!failRequests.isEmpty && (System.currentTimeMillis() - l < 6000000)) {
      val request: BatchMutateRequest = RequestFactory.of("Chocorotation").startBatchMutation().setTimeout(100000)
      val iterator: Iterator[(Key, Document)] = failRequests.iterator
      var count = 0
      val currentBatchSet: util.Set[Key] = new util.HashSet[Key]
      while (iterator.hasNext && count < 500) {
        val tuple: (Key, Document) = iterator.next()
        request.addForUpsert(tuple._1, tuple._2)
        count += 1
        currentBatchSet.add(tuple._1)
      }
      val response: BatchMutateResponse = request.execute()
      val failSet: util.Set[Key] = response.getInsertResponse.getErrors.keySet()
      failRequests = failRequests.filterKeys(key => {
        failSet.contains(key) || (!currentBatchSet.contains(key))
      })
      println("tmp fail size=" + failRequests.size())
      //Thread.sleep(2000)
    }
    println("time spend=" + (System.currentTimeMillis() - start) / 1000)
    println("final fail size=" + failRequests.size())
    for (key <- failRequests.keySet()) {
      println("fail key=" + key.getLastIdString)
    }


    MonstorClientUtil.shutdown()
  }

}

