package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.text.SimpleDateFormat
import java.util.Properties

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import com.ebay.traffic.monitoring.{ESMetrics, Field, Metrics}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col

object EpnNrtJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new EpnNrtJob(params)

    job.run()
    job.stop()
  }
}

class EpnNrtJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {

  lazy val outputDir = properties.getProperty("epnnrt.outputdir")
  lazy val epnNrtTempDir = outputDir + "/tmp/"
  lazy val METRICS_INDEX_PREFIX = "chocolate-metrics-"

  @transient lazy val schema_epn_click_table = TableSchema("df_epn_click.json")

  @transient lazy val schema_epn_impression_table = TableSchema("df_epn_impression.json")

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  @transient lazy val metadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("epnnrt.upstream.epn"))
    Metadata(params.workDir, ChannelType.EPN.toString, usage)
  }

  @transient lazy val batchSize: Int = {
    val batchSize = properties.getProperty("epnnrt.metafile.batchsize")
    if (StringUtils.isNumeric(batchSize)) {
      Integer.parseInt(batchSize)
    } else {
      10 // default to 10 metafiles
    }
  }

  @transient lazy val metrics: Metrics = {
    val url  = properties.getProperty("epnnrt.elasticsearchUrl")
    if (url != null && url.nonEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, url)
      ESMetrics.getInstance()
    } else null
  }

  override def run(): Unit = {
    //1. load meta files
    logger.info("load metadata...")

    var cappingMeta = metadata.readDedupeOutputMeta(".epnnrt")

    if (cappingMeta.length > batchSize) {
      cappingMeta = cappingMeta.slice(0, batchSize)
    }

    //init couchbase datasource
    CorpCouchbaseClient.dataSource = properties.getProperty("epnnrt.datasource")

    cappingMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        //2. load DataFrame
        val date = getDate(datesFile._1)
        var df = readFilesAsDFEx(datesFile._2)
        df = df.repartition(properties.getProperty("epnnrt.repartition").toInt)
        val epnNrtCommon = new EpnNrtCommon(params, df)
        logger.info("load DataFrame, date=" + date + ", with files=" + datesFile._2.mkString(","))

        val timestamp = df.first().getAs[Long]("timestamp")

        // filter click and impression data, and if there is filterTime, filter the data older than filter time
        var df_click = df.filter(col("channel_action") === "CLICK")
        var df_impression = df.filter(col("channel_action") === "IMPRESSION")

        val debug = properties.getProperty("epnnrt.debug").toBoolean

        var df_click_count_before_filter = 0L
        var df_impression_count_before_filter = 0L

        if (debug) {
          df_click_count_before_filter = df_click.count()
          df_impression_count_before_filter = df_impression.count()
        }

        logger.info("Current filter timestamp is: " + params.filterTime)
        var filtered = false
        if (!params.filterTime.equalsIgnoreCase("") && !params.filterTime.equalsIgnoreCase("0"))
          filtered = true

        if (filtered) {
          try {
              df_click = df_click.filter( r=> {
                r.getAs[Long]("timestamp") >= params.filterTime.toLong
              })
              df_impression = df_impression.filter( r=> {
                r.getAs[Long]("timestamp") >= params.filterTime.toLong
              })
          } catch {
            case e: Exception =>
              logger.error("Illegal filter timestamp: " + params.filterTime + e)
          }
        }


        var df_click_count_after_filter = 0L
        var df_impression_count_after_filter = 0L
        if (debug) {
          df_click_count_after_filter = df_click.count()
          df_impression_count_after_filter = df_impression.count()
          metrics.meter("ClickFilterCount", df_click_count_before_filter - df_click_count_after_filter)
          metrics.meter("ImpressionFilterCount", df_impression_count_before_filter - df_impression_count_after_filter)
        }

        //3. build impression dataframe  save dataframe to files and rename files
        var impressionDf = new ImpressionDataFrame(df_impression, epnNrtCommon).build()
        impressionDf = impressionDf.repartition(params.partitions)
        saveDFToFiles(impressionDf, epnNrtTempDir + "/impression/", "gzip", "csv", "tab")

        val countImpDf = readFilesAsDF(epnNrtTempDir + "/impression/", schema_epn_impression_table.dfSchema, "csv", "tab", false)

        metrics.meter("SuccessfulCount", countImpDf.count(), timestamp, Field.of[String, AnyRef]("channelAction", "IMPRESSION"))

        //write to EPN NRT output meta files
        val imp_files = renameFile(outputDir + "/impression/", epnNrtTempDir + "/impression/", date, "dw_ams.ams_imprsn_cntnr_cs_")
        val imp_metaFile = new MetaFiles(Array(DateFiles(date, imp_files)))

        try {
          metadata.writeOutputMeta(imp_metaFile, properties.getProperty("epnnrt.result.meta.imp.outputdir"), Array(".epnnrt"))
          logger.info("successfully write EPN NRT impression output meta to HDFS")
          metrics.meter("OutputMetaSuccessful", params.partitions, Field.of[String, AnyRef]("channelAction", "IMPRESSION"))

        } catch {
          case e: Exception => {
            logger.error("Error while writing EPN NRT impression output meta files" + e)
          }
        }

        //4. build click dataframe  save dataframe to files and rename files
        var clickDf = new ClickDataFrame(df_click, epnNrtCommon).build()
        clickDf = clickDf.repartition(params.partitions)

       // val enableLastViewItem = properties.getProperty("epnnrt.enablelastviewitem").toBoolean


        /*if(enableLastViewItem) {
          try {
            val executor: ExecutorService = Executors.newFixedThreadPool(maxThreadNum)
            val completionService: CompletionService[(String, String)] = new ExecutorCompletionService(executor)
             //clickDf = clickDf.mapPartitions(lastViewItemFunc(completionService))
            clickDf.rdd.mapPartitions(lastViewItemFunc(completionService))
          } catch {
            case e: Exception => {
              logger.error("Exception while getting last view item info from bullseye" + e)
              metrics.meter("BullsEyeError",1,timestamp)
            }
          }
        }*/

        saveDFToFiles(clickDf, epnNrtTempDir + "/click/", "gzip", "csv", "tab")

        val countClickDf = readFilesAsDF(epnNrtTempDir + "/click/", schema_epn_click_table.dfSchema, "csv", "tab", false)

        metrics.meter("SuccessfulCount", countClickDf.count(), timestamp, Field.of[String, AnyRef]("channelAction", "CLICK"))

        val clickFiles = renameFile(outputDir + "/click/", epnNrtTempDir + "/click/", date, "dw_ams.ams_clicks_cs_")


        // 5.delete the finished meta files
        metadata.deleteDedupeOutputMeta(file)

        if (metrics != null)
          metrics.flush()

        //6. write the epn-nrt meta output file to hdfs
        val click_metaFile = new MetaFiles(Array(DateFiles(date, clickFiles)))
        try {
          metadata.writeOutputMeta(click_metaFile, properties.getProperty("epnnrt.result.meta.click.outputdir"), Array(".epnnrt_1", ".epnnrt_2"))
          metrics.meter("OutputMetaSuccessful", params.partitions * 2, Field.of[String, AnyRef]("channelAction", "CLICK"))
          logger.info("successfully write EPN NRT Click output meta to HDFS, job finished")
        } catch {
          case e: Exception => {
            logger.error("Error while writing EPN NRT Click output meta files" + e)
          }
        }
      })
    })
  }

  def renameFile(outputDir: String, sparkDir: String, date: String, prefix: String) = {
    // rename result to output dir
    val dateOutputPath = new Path(outputDir + "/date=" + date)
    var max = -1
    if (fs.exists(dateOutputPath)) {
      val outputStatus = fs.listStatus(dateOutputPath)
      if (outputStatus.nonEmpty) {
        max = outputStatus.map(status => {
          val name = status.getPath.getName
          val number = name.substring(name.lastIndexOf("_"))
          Integer.valueOf(number.substring(1, number.indexOf(".")))
        }).sortBy(i => i).last
      }
    } else {
      fs.mkdirs(dateOutputPath)
    }

    val fileStatus = fs.listStatus(new Path(sparkDir))
    val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format max + 1 + swi._2).replace(" ", "0")
        val target = new Path(dateOutputPath, prefix +
          date.replaceAll("-", "") + "_" + sc.applicationId + "_" + seq + ".dat.gz")
        fs.rename(src, target)
        target.toString
      })
    files
  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }

  /*def lastViewItemFunc(completionService: CompletionService[(String, String)])(iter: Iterator[Row]): Iterator[Row] = {
    val list = List[Row]()
    while (iter.hasNext) {
      var threadNum = 0
      val r = iter.next()

      while (threadNum < maxThreadNum) {
       // completionService.submit(() => getLastViewItem(r.getAs[String]("CRLTN_GUID_TXT"), r.getAs[String]("USER_ID"), r.getAs[String]("CLICK_TS")))
        completionService.submit(new Callable[(String, String)] {
          override def call(): (String, String) = BullseyeUtils.getLastViewItem2(r.getAs[String]("CRLTN_GUID_TXT"), getTimeStamp(r.getAs[String]("CLICK_TS")))
        })
        threadNum = threadNum + 1
      }
      var received = 0
      while (received < threadNum) {
        val res = completionService.take().get()
        received = received + 1
        fixLastViewItem(res._1, res._2, r.getAs[String]("CRLTN_GUID_TXT"))
      }
    }
    list.iterator
  }*/

  //update last_view_item_id and last_view_item_ts column
/*  def fixLastViewItem(cguid: String, lstVitm: String, lstItmStmp: String): Unit = {
    /* r.getAs("LAST_VWD_ITEM_ID") = res(0)
           r.getAs("LAST_VWD_ITEM_TS") = res(1)*/
  }*/


  def getTimeStamp(date: String): Long = {
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      sdf.parse(date).getTime
    } catch {
      case e: Exception => {
        logger.error("Error while parsing timestamp " + e)
        0L
      }
    }
  }
}