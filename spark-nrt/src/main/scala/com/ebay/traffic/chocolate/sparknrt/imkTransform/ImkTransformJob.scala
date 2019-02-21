package com.ebay.traffic.chocolate.sparknrt.imkTransform

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkDump.TableSchema
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.compress.GzipCodec

import scala.io.Source

object ImkTransformJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkTransformJob(params)

    job.run()
    job.stop()
  }
}

class ImkTransformJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  @transient lazy val metadata: Metadata = Metadata(params.workDir, params.channel, MetadataEnum.imkDump)

  @transient lazy val schema_tfs = TableSchema("df_imk.json")
  @transient lazy val schema_apollo = TableSchema("df_imk_apollo.json")
  @transient lazy val schema_apollo_dtl = TableSchema("df_imk_apollo_dtl.json")
  @transient lazy val schema_apollo_mg = TableSchema("df_imk_apollo_mg.json")
  @transient lazy val mfe_name_id_map: Map[String, String] = {
    val mapData = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("mfe_name_id_map.txt")).getLines
    mapData.map(line => line.split("\\|")(0) -> line.split("\\|")(1)).toMap
  }

  lazy val imkTempDir: String = params.outputDir + "/imkTemp/"
  lazy val dtlTempDir: String = params.outputDir + "/dtlTemp/"
  lazy val mgTempDir: String = params.outputDir + "/mgTemp/"
  lazy val imkOutputDir: String = params.outputDir + "/imkOutput/"
  lazy val dtlOutputDir: String = params.outputDir + "/dtlOutput/"
  lazy val mgOutputDir: String = params.outputDir + "/mgOutput/"

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

    var imkDumpOutputMeta = metadata.readDedupeOutputMeta(".apollo")
    // at most 3 meta files
    if (imkDumpOutputMeta.length > 3) {
      imkDumpOutputMeta = imkDumpOutputMeta.slice(0, 3)
    }

    imkDumpOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        val date = datesFile._1
        var commonDf = readFilesAsDFEx(datesFile._2, schema_tfs.dfSchema, "csv", "bel")
          .withColumn("item_id", getItemIdUdf(col("roi_item_id"), col("item_id")))
          .withColumn("mfe_id", getMfeIdUdf(col("mfe_name")))
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

        // select data columns
        commonDf.select(schema_apollo.dfColumns: _*)
          .rdd
          .map(row => ("", row.mkString("\u007F")))
          .repartition(1)
          .saveAsSequenceFile(imkTempDir, compressCodec)

        // select dtl columns
        commonDf.select(schema_apollo_dtl.dfColumns: _*)
          .rdd
          .map(row => ("", row.mkString("\u007F")))
          .repartition(1)
          .saveAsSequenceFile(dtlTempDir, compressCodec)

        // select mg columns
        commonDf.select(schema_apollo_mg.dfColumns: _*)
          .filter($"mgvalue" =!= "")
          .rdd
          .map(row => ("", row.mkString("\u007F")))
          .repartition(1)
          .saveAsSequenceFile(mgTempDir, compressCodec)

        simpleRenameFiles(imkTempDir, imkOutputDir, date)
        simpleRenameFiles(dtlTempDir, dtlOutputDir, date)
        simpleRenameFiles(mgTempDir, mgOutputDir, date)

        fs.delete(new Path(imkTempDir), true)
        fs.delete(new Path(dtlTempDir), true)
        fs.delete(new Path(mgTempDir), true)

      })
      metadata.deleteDedupeOutputMeta(file)
      logger.info(s"processed:$file")
    })

  }

  val getItemIdUdf: UserDefinedFunction = udf((roi_item_id: String, item_id: String) => getItemId(roi_item_id, item_id))
  val getMfeIdUdf: UserDefinedFunction = udf((mfe_name: String) => getMfeIdByMfeName(mfe_name))

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

  /**
    * rename save files to data folder
    */
  def simpleRenameFiles(workDir: String, outputDir: String, date: String): Unit = {
    val status = fs.listStatus(new Path(workDir))
    status
      .filter(path => path.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format swi._2).replace(" ", "0")
        // chocolate_xxxxxxxx_appid_seq
        fs.rename(new Path(src.toString), new Path(outputDir + params.transformedPrefix + date + "_" + sc.applicationId + "_" + seq))
      })
  }

}
