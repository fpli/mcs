package com.ebay.traffic.chocolate.sparknrt.cb2nd

import java.lang.reflect.Field
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.view.{ViewQuery, ViewRow}
import com.ebay.dap.epic.monster.migrate.tool.auth.MonstorEnvEnum
import com.ebay.dap.epic.monster.migrate.tool.dao.MonstorClientUtil
import com.ebay.kernel.context.AppBuildConfig
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbaseV2.CorpCouchbaseClientV2
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import com.google.gson.Gson
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, from_unixtime, to_date}
import org.joda.time.DateTime
import org.monstor.client.RequestFactory
import org.monstor.client.operations.common.{Document, DocumentFactory, Filter, FilterFactory, Key, KeyFactory}
import org.monstor.client.operations.mutate.{BatchMutateRequest, BatchMutateResponse}
import org.monstor.client.operations.query.{QueryRequest, QueryResponse}
import rx.Observable
import rx.functions.Func1
import scalaj.http.Http

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object CB2NDTool extends App {
  override def main(args: Array[String]) = {
    val params = Parameter(args)

    val job = new CB2NDTool(params)

    job.run()
    job.stop()
  }
}

class CB2NDTool(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {
  override def run() = {
    var beginDate: String = params.begin
    var endDate: String = params.`end`
    if (StringUtils.isEmpty(beginDate)) {
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val calendar: Calendar = Calendar.getInstance()
      calendar.set(Calendar.MILLISECOND, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.HOUR_OF_DAY, 0)
      calendar.add(Calendar.DATE, 1)
      endDate=dateFormat.format(calendar.getTime)
      calendar.add(Calendar.DATE, -2)
      beginDate=dateFormat.format(calendar.getTime)
    }
    logger.info("sync data from "+beginDate+" to "+endDate)
    val couchbaseViewName = "rotation_info"
    val secretEndPoint: String = "https://trackingproxy.vip.ebay.com/tracking/proxy/v1/fidelius/secrets?path=trackingproxy/rotation/secret"
    var secret = ""
    val keyspace = "Chocorotation"
    val docSetName = "rotation"
    val batchSize = 50
    val mapper: ObjectMapper = new ObjectMapper()
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
    secret = Http(secretEndPoint).method("GET")
      .asString.body
    val start: Long = System.currentTimeMillis()
    val gson = new Gson
    val (cacheClient, bucket) = CorpCouchbaseClientV2.getBucketFunc()
    val query: ViewQuery = ViewQuery.from(couchbaseViewName, "last_update_time")
    // one year as interval
    var retry=0
    val viewResult: util.List[ViewRow] = new util.ArrayList[ViewRow]
    while(retry<3&&viewResult.size()==0) {
      val interval: Long = 1000L * 60L * 60L * 24L * 365L
      var begin: Long = new DateTime(beginDate).getMillis
      val end: Long = new DateTime(endDate).getMillis
      while (begin < end) {
        query.startKey(begin)
        if (end - begin < interval) {
          query.endKey(end)
        } else {
          query.endKey(begin + interval)
        }
        val rows: util.List[ViewRow] = bucket.query(query).allRows
        logger.info("size between " + begin + " to " + (begin + interval) + " =" + rows.size())
        viewResult.addAll(rows)
        begin += interval
      }
      retry+=1
    }
    if(viewResult.size()==0){
      throw new Exception("no data today!")
    }
    val keys: mutable.Buffer[String] = mutable.Buffer[String]()
    import scala.collection.JavaConversions._
    for (row <- viewResult) {
      keys += row.id()
    }
    val views: util.List[JsonDocument] = Observable
      .from(keys)
      .flatMap(new Func1[String, Observable[JsonDocument]]() {
        override def call(key: String): Observable[JsonDocument] = {
          bucket.async.get(key, classOf[JsonDocument])
        }
      }).toList.toBlocking.single
    logger.info("view size=" + views.size())
    val dbEnv: Field = AppBuildConfig.getInstance().getClass.getDeclaredField("dbEnv")
    dbEnv.setAccessible(true)
    dbEnv.set(AppBuildConfig.getInstance(), "prod")
    MonstorClientUtil.init("urn:ebay-marketplace-consumerid:181ad791-7ef4-4c84-96fe-691fd7aca187", secret, MonstorEnvEnum.PROD)
    val list: util.List[util.List[JsonDocument]] = Lists.partition(views, batchSize)
    var failRequests: util.Map[Key, Document] = new util.HashMap[Key, Document]
    for (i <- 0 until list.size()) {
      logger.info("batch=" + i)
      val request: BatchMutateRequest = RequestFactory.of(keyspace).startBatchMutation().setTimeout(99000)
      for (j <- 0 until list.get(i).size()) {
        val document: JsonDocument = list.get(i).get(j)
        val rotationInfo: RotationInfo = gson.fromJson(document.content.toString, classOf[RotationInfo])
        val d: Document = DocumentFactory.createDocument(mapper.valueToTree(rotationInfo))
        val key: Key = KeyFactory.create(rotationInfo.getRotation_string).add(docSetName, rotationInfo.getRotation_string)
        request.addForUpsert(key, d)
      }
      val response: BatchMutateResponse = request.execute()
      val total: util.Map[Key, Document] = request.getAllForUpserts
      val iterator: Iterator[Key] = response.getInsertResponse.getErrors.keySet().iterator
      while (iterator.hasNext) {
        val k: Key = iterator.next()
        failRequests.put(k, total.get(k))
      }
      logger.info("average batch time=" + ((System.currentTimeMillis() - start) / 1000) / (i + 1))
      Thread.sleep(1000)
    }
    logger.info("fail size=" + failRequests.size())
    val l: Long = System.currentTimeMillis()
    while (!failRequests.isEmpty && (System.currentTimeMillis() - l < 6000000)) {
      val request: BatchMutateRequest = RequestFactory.of(keyspace).startBatchMutation().setTimeout(99000)
      val iterator: Iterator[(Key, Document)] = failRequests.iterator
      var count = 0
      val currentBatchSet: util.Set[Key] = new util.HashSet[Key]
      while (iterator.hasNext && count < batchSize) {
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
      logger.info("tmp fail size=" + failRequests.size())
      Thread.sleep(1000)
    }
    logger.info("time spend=" + (System.currentTimeMillis() - start) / 1000)
    logger.info("final fail size=" + failRequests.size())
    for (key <- failRequests.keySet()) {
      logger.info("fail key=" + key.getLastIdString)
    }
    val monstorUtil = new MonstorClientUtil[RotationInfo](keyspace, 100, 20000)
    logger.info("keys size="+monstorUtil.listKeys(docSetName).size)


    val filter: Filter = FilterFactory.createFilter("last_update_time", Filter.Condition.LESS_THAN_OR_EQUAL, new DateTime(endDate).getMillis)
      .and(FilterFactory.createFilter("last_update_time", Filter.Condition.GREATER_THAN_OR_EQUAL, new DateTime(beginDate).getMillis))
    val request: QueryRequest = RequestFactory.of(keyspace).startQuery(docSetName).`match`(filter)
    var response: QueryResponse = null
    val infoList: util.List[RotationInfo] = new util.ArrayList[RotationInfo]
    try {
      do {
        response = if (response == null) request.batchSize(batchSize).setRetryIsAllowed(true).setTimeout(20000).execute
        else response.execute
        for (keyWithDocument <- response.getDocuments) {
          infoList.add(mapper.treeToValue(keyWithDocument.getDocument.getContent, classOf[RotationInfo])
          )
        }
      } while (response.hasMore)
    }
    catch {
      case e: Exception => {
        logger.info("query occurs exception=" + e)
      }
    }
    logger.info("query size=" + infoList.size())
    if(infoList.size()!=viewResult.size()){
      throw new Exception("data is not in sync. cb has "+viewResult.size()+" while nd has "+infoList.size())
    }
    var jsonArr = new ArrayBuffer[String]
    for(info<-infoList){
      jsonArr+=gson.toJson(info)
    }
    import spark.implicits._
    if(jsonArr.nonEmpty) {
      saveDFToFiles(spark.read.json(jsonArr.toDS())
        .drop("rotation_tag")
        .withColumn("date", to_date(col("update_date"))),
        "viewfs://apollo-rno/user/b_marketing_tracking/tracking-events/rotation",
        "snappy", "parquet", "del", headerHint = false, SaveMode.Overwrite, "date")
    }
    response.close()
    MonstorClientUtil.shutdown()
  }
}