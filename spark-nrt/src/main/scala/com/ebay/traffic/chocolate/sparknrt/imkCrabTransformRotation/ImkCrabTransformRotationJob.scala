package com.ebay.traffic.chocolate.sparknrt.imkCrabTransformRotation

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.types.StructType

object ImkCrabTransformRotationJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkCrabTransformRotationJob(params)

    job.run()
    job.stop()
  }
}

class ImkCrabTransformRotationJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode){

  @transient lazy val schema_apollo = TableSchema("df_imk_apollo.json")
  @transient lazy val schema_apollo_dtl = TableSchema("df_imk_apollo_dtl.json")
  @transient lazy val schema_apollo_mg = TableSchema("df_imk_apollo_mg.json")

  lazy val imkInputDir: String = params.inputDir + "/imkOutput/"
  lazy val dtlInputDir: String = params.inputDir + "/dtlOutput/"
  lazy val mgInputDir: String = params.inputDir + "/mgOutput/"

  lazy val imkRotationTempDir: String = params.outputDir + "/imkTemp/"
  lazy val dtlRotationTempDir: String = params.outputDir + "/dtlTemp/"
  lazy val mgRotationTempDir: String = params.outputDir + "/mgTemp/"

  lazy val imkRotationOutputDir: String = params.outputDir + "/imkOutput/"
  lazy val dtlRotationOutputDir: String = params.outputDir + "/dtlOutput/"
  lazy val mgRotationOutputDir: String = params.outputDir + "/mgOutput/"

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  override def run(): Unit = {
    rotation(imkInputDir, schema_apollo.dfSchema, imkRotationTempDir, imkRotationOutputDir)
    rotation(dtlInputDir, schema_apollo_dtl.dfSchema, dtlRotationTempDir, dtlRotationOutputDir)
    rotation(mgInputDir, schema_apollo_mg.dfSchema, mgRotationTempDir, mgRotationOutputDir)
  }

  def rotation(inputDir: String, schema: StructType, tempDir: String, outputDir: String): Unit = {
    fs.delete(new Path(tempDir), true)

    val status = fs.listStatus(new Path(inputDir))
    val dateRawFiles = status
      .map(s => s.getPath.toString)
      .map(path => {
        val prefixIndex = path.indexOf(params.transformedPrefix)
        val date = path.substring(prefixIndex + params.transformedPrefix.length, prefixIndex + params.transformedPrefix.length + "date=2019-09-01".length)
        (date, path)
      }).groupBy(_._1).map(p => p._1 -> p._2.map(_._2))

    dateRawFiles.foreach(str => {
      logger.info("rotation files "+ str._2.mkString(","))
      val rawFiles = str._2
      val frame = readFilesAsDFEx(rawFiles, schema, "sequence", "delete")
      frame.rdd.map(row => ("", row.mkString("\u007F"))).repartition(1).saveAsSequenceFile(tempDir, Some(classOf[GzipCodec]))
      simpleRenameFiles(tempDir, outputDir, str._1)
      fs.delete(new Path(tempDir), true)
      rawFiles.foreach(rawFile=> {
        logger.info("delete "+ rawFile)
        fs.delete(new Path(rawFile), true)
      })
    })
  }

  /**
    * rename files
    * @param tempDir tempDir
    * @param outputDir outputDir
    * @param date date
    */
  def simpleRenameFiles(tempDir: String, outputDir: String, date: String): Unit = {
    fs.listStatus(new Path(tempDir))
      .filter(path => path.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format swi._2).replace(" ", "0")
        // chocolate_appid_seq
        val fileName = outputDir + params.transformedPrefix + date + "_" + sc.applicationId + "_" + seq
        logger.info("rename from " + src + " to " + fileName)
        fs.rename(new Path(src.toString), new Path(fileName))
      })
  }

}
