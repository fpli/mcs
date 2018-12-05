package com.ebay.traffic.chocolate.job

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * Created by yliu29 on 11/8/17.
  *
  * Basic class for chocolate spark jobs
  */
abstract class BaseSparkJob(val jobName: String,
                            val mode: String = "yarn",
                            val enableHiveSupport: Boolean = false) extends Serializable {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Whether the spark job is in local mode
    */
  lazy val isLocal: Boolean = {
    mode.indexOf("local") == 0
  }

  /**
    * The spark session, which is the entrance point of DataFrame, DataSet and Spark SQL.
    */
  @transient lazy val spark = {
    val builder = SparkSession.builder().appName(jobName)

    if (isLocal) {
      builder.master(mode)
        .appName("SparkUnitTesting")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
      // for test, hive support is not enabled. Use in-memory catalog implementation
    } else if (enableHiveSupport) {
      builder.enableHiveSupport()
    }
    builder.getOrCreate()
  }

  /**
    * The spark context
    */
  @transient lazy val sc = {
    spark.sparkContext
  }

  /**
    * The java spark context
    */
  @transient lazy val jsc = {
    JavaSparkContext.fromSparkContext(sc);
  }

  /**
    * The sql context
    */
  @transient lazy val sqlsc = {
    spark.sqlContext;
  }

  /**
    * delimiters for file formats
    */
  lazy val YSep = delimiterMap("del")

  lazy val delimiterMap =
    Map("bel" -> "\007", "tab" -> "\t",
      "space" -> " ", "comma" -> ",", "del" -> "\177")

  /**
    * Counter for the corrupt rows.
    */
  @transient lazy val corruptRows = new ThreadLocal[Int]() {
    override def initialValue: Int = 0
  }
  lazy val MAX_CORRUPT_ROWS = 100

  /**
    * Split the row string to fields of string array using delimiter.
    *
    * @param line   the row string
    * @param colSep the delimiter
    * @return the fields
    */
  final def asRow(line: String, colSep: String = YSep): Array[String] = {
    line.split(colSep, -1)
  }

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  @throws(classOf[Exception])
  def run()

  /**
    * stop the spark job.
    */
  def stop() = {
    if (!isLocal) {
      println("spark_application_id_for_yarn_log=" + sc.applicationId)
    }
    spark.stop()
  }

  /**
    * Convert string array of row fields to DataFrame row
    * according to the table schema.
    *
    * @param values string array of row fields
    * @param schema dataframe schema
    * @return dataframe row
    */
  def toDfRow(values: Array[String], schema: StructType): Row = {
    require(values.length == schema.fields.length
      || values.length == schema.fields.length + 1)
    try {
      Row(values zip schema map (e => {
        if (e._1.length == 0) {
          null
        } else {
          e._2.dataType match {
            case _: StringType => e._1.trim
            case _: LongType => e._1.trim.toLong
            case _: IntegerType => e._1.trim.toInt
            case _: ShortType => e._1.trim.toShort
            case _: FloatType => e._1.trim.toFloat
            case _: DoubleType => e._1.trim.toDouble
            case _: ByteType => e._1.trim.toByte
            case _: BooleanType => e._1.trim.toBoolean
          }
        }
      }): _*)
    } catch {
      case ex: Exception => {
        corruptRows.set(corruptRows.get + 1)
        if (corruptRows.get() <= MAX_CORRUPT_ROWS) {
          logger.warn("Failed to parse row: " + values.mkString("|"), ex)
          null
        } else {
          logger.error("Two many corrupt rows.")
          throw ex
        }
      }
    }
  }

  /**
    * Read table files as Dataframe.
    *
    * @param inputPath     the input path of table files
    * @param schema        the dataframe schema of table
    * @param inputFormat   the input file format, it can be "parquet", "orc", "csv", "sequence"
    * @param delimiter     the delimiter for fields in the file,
    *                      the value can be one of 'bel', 'tab', 'space', 'comma', 'del'.
    * @param broadcastHint whether to broadcast the dataframe
    * @return the dataframe
    */
  def readFilesAsDF(inputPath: String, schema: StructType = null,
                    inputFormat: String = "parquet", delimiter: String = "del",
                    broadcastHint: Boolean = false): DataFrame = {
    readFilesAsDFEx(Array(inputPath), schema, inputFormat, delimiter, broadcastHint)
  }

  /**
    * Read table files as Dataframe.
    *
    * @param inputPaths    the input paths of table files
    * @param schema        the dataframe schema of table
    * @param inputFormat   the input file format, it can be "parquet", "orc", "csv", "sequence"
    * @param delimiter     the delimiter for fields in the file,
    *                      the value can be one of 'bel', 'tab', 'space', 'comma', 'del'.
    * @param broadcastHint whether to broadcast the dataframe
    * @return the dataframe
    */
  def readFilesAsDFEx(inputPaths: Array[String], schema: StructType = null,
                    inputFormat: String = "parquet", delimiter: String = "del",
                    broadcastHint: Boolean = false): DataFrame = {
    require(delimiterMap.contains(delimiter),
      "the value of delimiter can be one of 'bel', 'tab', 'space', 'comma', 'del'")
    val df = inputFormat match {
      case "parquet" => spark.read.parquet(inputPaths: _*)
      case "orc" => {
        spark.conf.set("spark.sql.orc.filterPushdown", "true")
        spark.read.orc(inputPaths: _*)
      }

      /**
        * case "csv" => spark.read.format("com.databricks.spark.csv")
        * .option("delimiter", delimiterMap(delimiter))
        * .schema(schema)
        * .load(inputPath)
        */
      case "csv" => {
        spark.createDataFrame(sc.textFile(inputPaths.mkString(","))
          .map(asRow(_, delimiterMap(delimiter)))
          .map(toDfRow(_, schema)).filter(_ != null), schema)
      }
      case "sequence" => {
        spark.createDataFrame(sc.sequenceFile[String, String](inputPaths.mkString(","))
          .values.map(asRow(_, delimiterMap(delimiter)))
          .map(toDfRow(_, schema)).filter(_ != null), schema)
      }
    }
    if (broadcastHint) broadcast(df) else df
  }

  /**
    * Read table files as RDD[Array[String]]
    * This method is used by readRDDAsDF.
    *
    * This method is deprecated, recommend to use readFilesAsDF
    *
    * @param inputPath   the input path of table files
    * @param inputFormat the input file format, it can be "csv", "sequence"
    * @param delimiter   the delimiter for fields in the file,
    *                    the value can be one of 'bel', 'tab', 'space', 'comma', 'del'.
    * @return the RDD
    */
  @deprecated
  def readFilesAsRDD(inputPath: String, inputFormat: String = "sequence",
                     delimiter: String = "del"): RDD[Array[String]] = {
    require(Seq("sequence", "csv").contains(inputFormat), "Unsupported file format: " + inputFormat)
    require(delimiterMap.contains(delimiter),
      "the value of delimiter can be one of 'bel', 'tab', 'space', 'comma', 'del'")
    inputFormat match {
      case "sequence" => sc.sequenceFile[String, String](inputPath).values
        .map(asRow(_, delimiterMap(delimiter)))
      case "csv" => sc.textFile(inputPath).map(asRow(_, delimiterMap(delimiter)))
    }
  }

  import scala.reflect.runtime.universe._

  /**
    * Read table files as Dataframe.
    * It internally reads the table files as RDD[Array[String]] first,
    * then it creates the Dataframe using the RDD.
    *
    * This method is deprecated, recommend to use readFilesAsDF
    */
  @deprecated
  def readRDDAsDF[U <: Product : TypeTag : ClassTag](inputPath: String,
                                                     inputFormat: String = "sequence",
                                                     delimiter: String = "del",
                                                     filterFuncOpt: Option[Array[String] => Boolean] = None,
                                                     mapFunc: Array[String] => U,
                                                     alias: String = null,
                                                     broadcastHint: Boolean = false): DataFrame = {
    val df = spark.createDataFrame[U](
      filterFuncOpt match {
        case Some(f) => readFilesAsRDD(inputPath, inputFormat, delimiter)
          .filter(f).map(mapFunc).filter(v => v != null)
        case None => readFilesAsRDD(inputPath, inputFormat, delimiter)
          .map(mapFunc).filter(v => v != null)
      }
    )
    val ret = if (alias == null) df else df.as(alias)
    if (broadcastHint) broadcast(ret) else ret
  }

  /**
    * Saves the content of the dataframe as the specified table.
    *
    * This method is deprecated, recommend to use saveDFToFiles
    *
    * @note for csv, we ignore the compression format,
    *       since compressed csv is not splittable and will affect performance
    * @param df              the dataframe
    * @param tableName       the table name to save as
    * @param partitionColumn the column to partition by before saving
    * @param outputFormat    the output file format, it can be "csv", "parquet", "orc"
    * @param writeMode       the save mode
    * @param delimiter       the delimiter for fields in the file,
    *                        the value can be one of 'bel', 'tab', 'space', 'comma', 'del'.
    * @param compressFormat  the compression codec used to compress the output
    */
  @deprecated
  def saveAsTable(df: DataFrame, tableName: String,
                  partitionColumn: String = null,
                  outputFormat: String = "parquet",
                  writeMode: SaveMode = SaveMode.Overwrite,
                  delimiter: String = "tab",
                  compressFormat: String = "snappy") = {
    require(delimiterMap.contains(delimiter),
      "the value of delimiter can be one of 'bel', 'tab', 'space', 'comma', 'del'")
    require(enableHiveSupport, "should enable hive support")

    val writer = df.write.mode(writeMode)

    outputFormat match {
      case "parquet" =>
        writer.format(outputFormat)
        spark.conf.set("spark.sql.parquet.compression.codec", compressFormat)
      case "orc" => {
        writer.format(outputFormat)
        spark.conf.set("spark.sql.orc.filterPushdown", "true")
        spark.conf.set("orc.compress", compressFormat)
      }
      case "csv" => {
        writer.format("com.databricks.spark.csv")
          .option("delimiter", delimiterMap(delimiter))
          .option("escape", null)
          .option("quoteMode", "NONE")
          .option("nullValue", "")
      }
    }

    if (partitionColumn != null) {
      writer.partitionBy(partitionColumn)
    }

    writer.saveAsTable(tableName)
  }

  /**
    * Save Dataframe to files
    *
    * @note for csv, it's recommended not use the compression format,
    *       since compressed csv is not splittable and will affect performance
    * @param df              the dataframe
    * @param outputPath      the output path
    * @param compressFormat  the compression codec used to compress the output
    * @param outputFormat    the output file format
    * @param delimiter       the delimiter for fields in output file,
    *                        the value can be one of 'bel', 'tab', 'space', 'comma', 'del'.
    * @param headerHint      output the header for csv
    * @param writeMode       the save mode
    * @param partitionColumn the column to partition by before saving
    */
  def saveDFToFiles(df: DataFrame, outputPath: String, compressFormat: String = "snappy",
                    outputFormat: String = "parquet", delimiter: String = "del",
                    headerHint: Boolean = false, writeMode: SaveMode = SaveMode.Overwrite,
                    partitionColumn: String = null) = {
    require(Seq("parquet", "csv", "orc").contains(outputFormat),
      "Unsupported storage format: " + outputFormat)
    require(delimiterMap.contains(delimiter),
      "the value of delimiter can be one of 'bel', 'tab', 'space', 'comma', 'del'")

    val writer = df.write.mode(writeMode)
    if (partitionColumn != null) {
      writer.partitionBy(partitionColumn)
    }
    outputFormat match {
      case "parquet" => {
        // acceptable values include: uncompressed, snappy, gzip, lzo
        spark.conf.set("spark.sql.parquet.compression.codec", compressFormat)
        writer.format(outputFormat).save(outputPath)
      }
      case "orc" => {
        spark.conf.set("spark.sql.orc.filterPushdown", "true")
        spark.conf.set("orc.compress", compressFormat) // zlib, snappy, none
        spark.conf.set("spark.io.compression.codec", compressFormat) // lzf, lz4, snappy
        writer.format(outputFormat).save(outputPath)
      }
      case "csv" => {
        writer.format("com.databricks.spark.csv")
          .option("delimiter", delimiterMap(delimiter))
          .option("header", headerHint.toString)
          .option("escape", null)
          .option("quote", "")
          .option("quoteMode", "NONE")
          .option("nullValue", "")

        if (compressFormat != null && !compressFormat.isEmpty && compressFormat != "uncompressed") {
          // acceptable values include: bzip2, gzip, lz4, snappy
          val codec = compressFormat match {
            case "bzip2" => "org.apache.hadoop.io.compress.BZip2Codec"
            case "gzip" => "org.apache.hadoop.io.compress.GzipCodec"
            case "lz4" => "org.apache.hadoop.io.compress.Lz4Codec"
            case "snappy" => "org.apache.hadoop.io.compress.SnappyCodec"
            case _ => throw new IllegalArgumentException("Unsupported storage format: " + compressFormat)
          }
          writer.option("codec", codec)
        }
        writer.save(outputPath)
      }
    }
  }
}