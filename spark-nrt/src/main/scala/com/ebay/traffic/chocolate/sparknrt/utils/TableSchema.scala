package com.ebay.traffic.chocolate.sparknrt.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.types._

/**
  * Created by yliu29 on 4/26/17.
  *
  * TableSchema contains the schema information of table.
  * It can be constructed from table schema json file.
  *
  * It's recommended to define all table schema through json files which are configurable.
  *
  * Usage:
  *   lazy val schema_tableA = TableSchema("schema_tableA.json")
  */
class TableSchema (val table: String,
                   val columns: Array[ColumnSchema],
                   val aliases: Array[AliasSchema]) extends Serializable {

  @transient lazy val types = Map(
    "string" -> StringType, "decimal"-> StringType, "long" -> LongType,
    "integer" -> IntegerType, "short" -> ShortType, "float" -> FloatType,
    "double" -> DoubleType, "byte" -> ByteType, "boolean" -> BooleanType,
    "date" -> DateType, "timestamp" -> TimestampType
  )

  /**
    * dataframe schema
    */
  @transient lazy val dfSchema: StructType = if (columns != null) {
    StructType(columns.map(e => StructField(e.name, types(e.dataType))))
  } else null

  /**
    * The column names of table
    */
  @transient lazy val columnNames: Array[String] = if (columns != null) {
    columns.map(_.name)
  } else null

  /**
    * The "Column"s of table
    */
  @transient lazy val dfColumns: Array[Column] = if (columns != null) {
    columns.map(e => new ColumnName(e.name))
  } else null

  /**
    * The column aliases
    */
  @transient lazy val columnAliases: Array[(String, String)] = if (aliases != null) {
    aliases.map(e => (e.name, e.alias))
  } else null

  /**
    * Default values of the table columns
    */
  @transient lazy val defaultValues: Map[String, Any] = if (columns != null) {
    columns.map(e => (e.name, e.default)).toMap
  } else null

  /**
    * Filter not the unwanted column names
    *
    * @param unwanted the unwanted column names
    * @return the remaining column names of table
    */
  def filterNotColumns(unwanted: Array[String]): Array[String] = if (columns != null) {
    columnNames.filterNot(unwanted.toSet)
  } else null
}

/**
  * Construct TableSchema from table schema json file.
  */
object TableSchema {
  private val jsonMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(DefaultScalaModule)

  def apply(file: String): TableSchema = {
    val url = getClassLoader.getResource(file)
    if (url != null) {
      jsonMapper.readValue(url, classOf[TableSchema])
    } else {
      new TableSchema(null, null, null)
    }
  }

  /** the classloader */
  def getClassLoader() = {
    var classLoader = Thread.currentThread().getContextClassLoader()
    if (classLoader == null) {
      classLoader = getClass.getClassLoader
    }
    classLoader
  }
}

/**
  * Table Column schema
  */
case class ColumnSchema(name: String, dataType: String, default: Any)

case class AliasSchema(name: String, alias: String)