package com.ebay.traffic.chocolate.sparknrt.imkCrabTransformOutputMerge

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.types.StructType

/**
 * Merge small imk crab transform output files to single file.
 *
 * @author Zhiyuan Wang
 * @since 2019-09-03
 */
object ImkCrabTransformOutputMergeJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkCrabTransformOutputMergeJob(params)

    job.run()
    job.stop()
  }
}

class ImkCrabTransformOutputMergeJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode){

  val compressCodec: Option[Class[GzipCodec]] = {
    if (params.compressOutPut) {
      Some(classOf[GzipCodec])
    } else {
      None
    }
  }

  @transient lazy val schema_apollo = TableSchema("df_imk_apollo.json")
  @transient lazy val schema_apollo_dtl = TableSchema("df_imk_apollo_dtl.json")
  @transient lazy val schema_apollo_mg = TableSchema("df_imk_apollo_mg.json")

  // imk crab transform output dir
  lazy val imkInputDir: String = params.inputDir + "/imkOutput"
  lazy val dtlInputDir: String = params.inputDir + "/dtlOutput"
  lazy val mgInputDir: String = params.inputDir + "/mgOutput"

  // mk crab transform backup dir
  lazy val imkBackupDir: String = params.backupDir + "/imkOutput"
  lazy val dtlBackupDir: String = params.backupDir + "/dtlOutput"
  lazy val mgBackupDir: String = params.backupDir + "/mgOutput"

  lazy val imkMergedTempDir: String = params.outputDir + "/imkTemp"
  lazy val dtlMergedTempDir: String = params.outputDir + "/dtlTemp"
  lazy val mgMergedTempDir: String = params.outputDir + "/mgTemp"

  lazy val imkMergedOutputDir: String = params.outputDir + "/imkOutput"
  lazy val dtlMergedOutputDir: String = params.outputDir + "/dtlOutput"
  lazy val mgMergedOutputDir: String = params.outputDir + "/mgOutput"

  override def run(): Unit = {
    mergeFiles(imkInputDir, schema_apollo.dfSchema, imkMergedTempDir, imkMergedOutputDir, imkBackupDir)
    mergeFiles(dtlInputDir, schema_apollo_dtl.dfSchema, dtlMergedTempDir, dtlMergedOutputDir, dtlBackupDir)
    mergeFiles(mgInputDir, schema_apollo_mg.dfSchema, mgMergedTempDir, mgMergedOutputDir, mgBackupDir)
  }

  def mergeFiles(inputDir: String, schema: StructType, tempDir: String, outputDir: String, backupDir: String): Unit = {
    fs.delete(new Path(tempDir), true)

    val status = fs.listStatus(new Path(inputDir))
    // group by files by date, merge files with the same date
    val dateRawPaths: Map[String, Array[String]] = status
      .map(s => s.getPath.toString)
      .map(file => {
        // eg file is chocolate_date=2019-09-02_application_1561139602691_263554_00001
        val prefixIndex = file.indexOf(params.transformedPrefix)
        val date = file.substring(prefixIndex + params.transformedPrefix.length, prefixIndex + params.transformedPrefix.length + "date=2019-09-01".length)
        (date, file)
      }).groupBy(_._1).map(p => p._1 -> p._2.map(_._2))

    dateRawPaths.foreach(tuple => {
      val rawFiles = tuple._2
      val frame = readFilesAsDFEx(rawFiles, schema, "sequence", "delete")
      logger.info("merge files "+ tuple._2.mkString(","))
      frame.rdd.map(row => ("", row.mkString("\u007F"))).repartition(1).saveAsSequenceFile(tempDir, compressCodec)
      simpleRenameFiles(tempDir, outputDir, tuple._1)
      fs.delete(new Path(tempDir), true)
      backupRawFiles(rawFiles, backupDir)
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
        val fileName = outputDir + "/" + params.transformedPrefix + date + "_" + sc.applicationId + "_" + seq
        logger.info("rename from " + src + " to " + fileName)
        fs.rename(new Path(src.toString), new Path(fileName))
      })
  }

  /**
   * backup imk crab transform files
   * @param rawFiles imk crab transform output files
   * @param backupDir backup dir
   */
  def backupRawFiles(rawFiles: Array[String], backupDir: String): Unit = {
    rawFiles.foreach(rawFile => {
      val srcPath = new Path(rawFile)
      val dstPath = new Path(backupDir + "/" + srcPath.getName)
      logger.info("backup from " + srcPath + " to " + dstPath)
      fs.rename(srcPath, dstPath)
    })
  }

}
