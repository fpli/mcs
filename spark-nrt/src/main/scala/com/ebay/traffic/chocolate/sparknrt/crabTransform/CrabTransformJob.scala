package com.ebay.traffic.chocolate.sparknrt.crabTransform

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import com.ebay.traffic.chocolate.sparknrt.utils.{MyID, TableSchema, XIDResponse}
import com.ebay.traffic.monitoring.{ESMetrics, Field, Metrics}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, _}
import scalaj.http.Http
import spray.json._

import scala.io.Source

object CrabTransformJob extends App {
  override def main(args: Array[String]) = {
    val params = Parameter(args)

    val job = new CrabTransformJob(params)

    job.run()
    job.stop()
  }
}

class CrabTransformJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode){

  @transient lazy val metadata: Metadata = {
    if (params.metaFile == "imkDump") {
      Metadata(params.workDir, params.channel, MetadataEnum.imkDump)
    } else {
      Metadata(params.workDir, params.channel, MetadataEnum.dedupe)
    }
  }

  @transient lazy val schema_tfs = TableSchema("df_imk.json")
  @transient lazy val schema_apollo = TableSchema("df_imk_apollo.json")
  @transient lazy val schema_apollo_dtl = TableSchema("df_imk_apollo_dtl.json")
  @transient lazy val schema_apollo_mg = TableSchema("df_imk_apollo_mg.json")

  @transient lazy val mfe_name_id_map: Map[String, String] = {
    val mapData = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("mfe_name_id_map.txt")).getLines
    mapData.map(line => line.split("\\|")(0) -> line.split("\\|")(1)).toMap
  }

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("crab_transform.properties"))
    properties
  }

  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      if (params.metaFile == "imkDump") {
        ESMetrics.init("imk-metrics-", params.elasticsearchUrl)
        ESMetrics.getInstance()
      } else {
        ESMetrics.init("crab-metrics-", params.elasticsearchUrl)
        ESMetrics.getInstance()
      }
    } else null
  }

  @transient lazy val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  lazy val imkTempDir: String = params.outputDir + "/" + params.channel + "/imkTemp/"
  lazy val dtlTempDir: String = params.outputDir + "/" + params.channel + "/dtlTemp/"
  lazy val mgTempDir: String = params.outputDir + "/" + params.channel + "/mgTemp/"
  lazy val imkOutputDir: String = params.outputDir + "/imkOutput/"
  lazy val dtlOutputDir: String = params.outputDir + "/dtlOutput/"
  lazy val mgOutputDir: String = params.outputDir + "/mgOutput/"
  lazy val xidHost: String = properties.getProperty("xid.xidHost")
  lazy val xidConsumerId: String = properties.getProperty("xid.xidConsumerId")
  lazy val xidClientId: String = properties.getProperty("xid.xidClientId")
  lazy val xidConnectTimeout: Int = properties.getProperty("xid.xidConnectTimeout").toInt
  lazy val xidReadTimeout: Int = properties.getProperty("xid.xidReadTimeout").toInt
  lazy val joinKeyword: Boolean = params.joinKeyword

  import spark.implicits._

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  override def run(): Unit = {
    val compressCodec = {
      if (params.compressOutPut) {
        Some(classOf[GzipCodec])
      } else {
        None
      }
    }

    // reduce the read parquet test size by increasing the reading input block size to 1GB
    // spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.minsize", "1073741824")
    // spark.sparkContext.hadoopConfiguration.set("mapred.min.split.size", "1073741824")

    fs.delete(new Path(imkTempDir), true)
    fs.delete(new Path(dtlTempDir), true)
    fs.delete(new Path(mgTempDir), true)

    var crabTransformMeta = metadata.readDedupeOutputMeta()
    // at most meta files
    if (crabTransformMeta.length > params.maxMetaFiles) {
      metrics.meter("imk.transform.TooManyMetas", crabTransformMeta.length, Field.of[String, AnyRef]("channelType", params.channel))
      crabTransformMeta = crabTransformMeta.slice(0, params.maxMetaFiles)
    }

    val partitions = crabTransformMeta.length

//    // add listener on task end to flush metrics
//    spark.sparkContext.addSparkListener(new SparkListener() {
//      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
//        if (metrics != null) {
//          metrics.flush()
//          metrics.close()
//        }
//      }
//    })

    val metas = mergeMetaFiles(crabTransformMeta)
    metas.foreach(f = datesFile => {
      val date = datesFile._1
      var commonDf = readFilesAsDFEx(datesFile._2, schema_tfs.dfSchema, "csv2", "bel")
        .repartition(params.xidParallelNum)
        .withColumn("item_id", getItemIdUdf(col("roi_item_id"), col("item_id")))
        .withColumn("user_id", getUserIdUdf(col("user_id"), col("cguid"), col("rvr_cmnd_type_cd")))
        .withColumn("mfe_id", getMfeIdUdf(col("mfe_name")))
        .withColumn("event_ts", setMessageLagUdf(col("event_ts")))
        .withColumn("mgvalue_rsn_cd", getMgvalueRsnCdUdf(col("mgvaluereason")))
        .na.fill(schema_tfs.defaultValues).cache()
      // set default values for some columns
      schema_apollo.filterNotColumns(commonDf.columns).foreach(e => {
        commonDf = commonDf.withColumn(e, lit(schema_apollo.defaultValues(e)))
      })
      schema_apollo_dtl.filterNotColumns(commonDf.columns).foreach(e => {
        commonDf = commonDf.withColumn(e, lit(schema_apollo_dtl.defaultValues(e)))
      })
      schema_apollo_mg.filterNotColumns(commonDf.columns).foreach(e => {
        commonDf = commonDf.withColumn(e, lit(schema_apollo_mg.defaultValues(e)))
      })

      // flush metrics for tasks
      import org.apache.spark.sql.catalyst.encoders.RowEncoder
      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(commonDf.schema)
      commonDf = commonDf.mapPartitions((iter: Iterator[Row]) => {
        if (metrics != null) {
          metrics.flush()
        }
        iter
      })

      if (joinKeyword) {
        val kwLKPDf = readFilesAsDF(params.kwDataDir).filter($"is_dup" === false)
        // select core data columns
        val coreDf = commonDf.select(schema_apollo.dfColumns: _*).drop("kw_id")
        val smallJoinDf = coreDf.select("keyword", "rvr_id")
          .withColumnRenamed("rvr_id", "temp_rvr_id")
          .filter(kwIsNotEmptyUdf(col("keyword"))).distinct()

        // if input number is less, then we choose broadcast join to improve the performance
        // 1,000,000 tfs file format data approximately equals to 500MB
        var isBroadCast = smallJoinDf.count() <= 1000000
        val heavyJoinResultDf = getJoinedKwDf(smallJoinDf, kwLKPDf, isBroadCast)

        coreDf.join(heavyJoinResultDf, $"rvr_id" === $"temp_rvr_id", "left_outer")
          .withColumn("kw_id", setDefaultValueForKwIdUdf(col("kw_id")))
          .select(schema_apollo.dfColumns: _*)
          .rdd
          .map(row => ("", row.mkString("\u007F")))
          .repartition(partitions)
          .saveAsSequenceFile(imkTempDir, compressCodec)
      } else {
        val coreDf = commonDf.select(schema_apollo.dfColumns: _*)
        coreDf.rdd
          .map(row => ("", row.mkString("\u007F")))
          .repartition(partitions)
          .saveAsSequenceFile(imkTempDir, compressCodec)
      }

      // select dtl columns
      commonDf.select(schema_apollo_dtl.dfColumns: _*)
        .rdd
        .map(row => ("", row.mkString("\u007F")))
        .repartition(partitions)
        .saveAsSequenceFile(dtlTempDir, compressCodec)

      // select mg columns
      commonDf.select(schema_apollo_mg.dfColumns: _*)
        .filter($"mgvalue" =!= "")
        .rdd
        .map(row => ("", row.mkString("\u007F")))
        .repartition(partitions)
        .saveAsSequenceFile(mgTempDir, compressCodec)

      simpleRenameFiles(imkTempDir, imkOutputDir, date)
      simpleRenameFiles(dtlTempDir, dtlOutputDir, date)
      simpleRenameFiles(mgTempDir, mgOutputDir, date)

      fs.delete(new Path(imkTempDir), true)
      fs.delete(new Path(dtlTempDir), true)
      fs.delete(new Path(mgTempDir), true)
    })
    //delete meta files
    crabTransformMeta.foreach(metaFiles => {
      val file = metaFiles._1
      metadata.deleteDedupeOutputMeta(file)
    })
    metrics.meter("imk.transform.processedMete", crabTransformMeta.length, Field.of[String, AnyRef]("channelType", params.channel))
    if (metrics != null) {
      metrics.flush()
      metrics.close()
    }
  }

  // join keyword table
  def getJoinedKwDf(smallJoinDf: DataFrame, kwLKPDf: DataFrame, isBroadcast: Boolean) : DataFrame = {
    if(isBroadcast) {
      kwLKPDf.join(broadcast(smallJoinDf), $"keyword" === $"kw", "inner")
        .withColumnRenamed("keyword", "temp_kw")
    } else {
      kwLKPDf.join(smallJoinDf, $"keyword" === $"kw", "inner")
        .withColumnRenamed("keyword", "temp_kw")
    }
  }

  val getItemIdUdf: UserDefinedFunction = udf((roi_item_id: String, item_id: String) => getItemId(roi_item_id, item_id))
  val getMfeIdUdf: UserDefinedFunction = udf((mfe_name: String) => getMfeIdByMfeName(mfe_name))
  val getMgvalueRsnCdUdf: UserDefinedFunction = udf((mgvaluereason: String) => getMgvalueRsnCd(mgvaluereason))
  val setDefaultValueForKwIdUdf: UserDefinedFunction = udf((kw_id: String) => {
    if (StringUtils.isEmpty(kw_id)) {
      "-999"
    } else {
      kw_id
    }
  })
  val kwIsNotEmptyUdf: UserDefinedFunction = udf((keyword: String) => StringUtils.isNotEmpty(keyword))
  val getUserIdUdf: UserDefinedFunction = udf((userId: String, cguid: String, cmndType: String) => getUserIdByCguid(userId, cguid, cmndType))
  val setMessageLagUdf: UserDefinedFunction = udf((eventTs: String) => setMessageLag(eventTs))
  /**
    * merge multi meta files to one, in case of run multi jobs
    * @param originMeta multi meta files
    * @return merged meta file
    */
  def mergeMetaFiles(originMeta: Array[(String, Map[String, Array[String]])]): Map[String, Array[String]] = {
    val result = new util.HashMap[String, Array[String]]
    originMeta.foreach(metaIter => {
      val metas = metaIter._2
      metas.foreach(meta => {
        val date = meta._1
        var tempArray = meta._2.map(file => {
          if(file.startsWith("hdfs")) {
            file
          } else {
            params.hdfsUri + file
          }
        })
        if (result.containsKey(date)) {
          tempArray = tempArray ++ result.get(date)
          result.remove(date)
          result.put(date, tempArray)
        } else {
          result.put(date, tempArray)
        }
      })
    })
    import scala.collection.JavaConverters._
    result.asScala.toMap
  }

  /**
    * set message lag
    * @param eventTs message event_ts
    * @return
    */
  def setMessageLag(eventTs: String): String = {
    if (StringUtils.isNotEmpty(eventTs)) {
      try{
        val messageDt = dateFormat.parse(eventTs)
        val nowDt = new Date()
        metrics.mean("imk.transform.messageLag", nowDt.getTime - messageDt.getTime, Field.of[String, AnyRef]("channelType", params.channel))
      } catch {
        case e:Exception => {
          logger.warn("parse event ts error", e)
        }
      }
    }
    eventTs
  }
  /**
    * set value for userid.
    * 1, use origin userid if it's not empty.
    * 2, use cguid to call ERS to get user id
    * @param userId origin user id
    * @param cguid cguid
    * @return user id
    */
  def getUserIdByCguid(userId: String, cguid: String, cmndType: String): String = {
    if (StringUtils.isEmpty(cmndType) || cmndType.equals("4")) {
      return userId
    }
    var result = userId
    if (StringUtils.isEmpty(userId) || userId.equals("0")) {
      if (StringUtils.isNotEmpty(cguid)) {
        try{
          metrics.meter("imk.transform.XidTryGetUserId", 1, Field.of[String, AnyRef]("channelType", params.channel))
          val xid = xidRequest("cguid", cguid)
          if (xid.accounts.nonEmpty) {
            metrics.meter("imk.transform.XidGotUserId", 1, Field.of[String, AnyRef]("channelType", params.channel))
            result = xid.accounts.head
          }
        } catch {
          case e: Exception => {
            logger.warn("call xid error" + e.printStackTrace())
          }
        }
      }
    }
    result
  }

  /**
    * call xid service to get userid by cguid
    * @param idType cguid, gadid, idfa, account
    * @param id
    * @return
    */
  def xidRequest(idType: String, id: String): MyID = {
    Http(s"http://$xidHost/anyid/v1/$idType/$id")
      .header("X-EBAY-CONSUMER-ID", xidConsumerId)
      .header("X-EBAY-CLIENT-ID", xidClientId)
      .timeout(xidConnectTimeout, xidReadTimeout)
      .asString
      .body
      .parseJson
      .convertTo[XIDResponse]
      .toMyID()
  }

  /**
    * set apollo item_id filed by tfs item_id and roi_item_id
    * @param roi_item_id roi_item_id
    * @param item_id item_id
    * @return apollo item_id
    */
  def getItemId(roi_item_id: String, item_id: String): String = {
    if (StringUtils.isNotEmpty(roi_item_id) && StringUtils.isNumeric(roi_item_id) && roi_item_id.toLong != -999) {
      roi_item_id
    } else if (StringUtils.isNotEmpty(item_id) && item_id.length <= 18) {
      item_id
    } else{
      ""
    }
  }

  /**
    * get mfe id by mfe name
    * @param mfeName mfe name
    * @return
    */
  def getMfeIdByMfeName(mfeName: String): String = {
    if (StringUtils.isNotEmpty(mfeName)) {
      mfe_name_id_map.getOrElse(mfeName, "-999")
    } else {
      "-999"
    }
  }

  def getMgvalueRsnCd(mgvaluereason: String): String = {
    if ("4".equalsIgnoreCase(mgvaluereason) || "BOT".equalsIgnoreCase(mgvaluereason)) {
      "4"
    } else {
      ""
    }
  }

  /**
    * rename files
    * @param workDir workdir
    * @param outputDir outputdir
    * @param date date
    */
  def simpleRenameFiles(workDir: String, outputDir: String, date: String): Unit = {
    val status = fs.listStatus(new Path(workDir))
    status
      .filter(path => path.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format swi._2).replace(" ", "0")
        // chocolate_appid_seq
        val fileName = params.transformedPrefix + date + "_" + sc.applicationId + "_" + seq
        fs.rename(new Path(src.toString), new Path(outputDir + "/" + fileName))
      })
  }

}
