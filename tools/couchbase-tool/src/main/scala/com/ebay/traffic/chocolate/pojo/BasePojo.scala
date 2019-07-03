package com.ebay.traffic.chocolate.pojo

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import com.ebay.traffic.chocolate.utils.ColumnDef


/**
  * Created by zhofan on 2019/06/11.
  */
trait BasePojo extends Serializable {
  val columnDefs: Array[ColumnDef]

  final lazy val fields: Array[StructField] = columnDefs.map(cd => StructField(cd.columnName, cd.dataType))

  final lazy val primaryFieldNames: Array[String] = columnDefs.filter(_.isPrimaryKey).map(_.columnName)

  final lazy val fieldNames: Array[String] = columnDefs.map(_.columnName)

  final lazy val structType: StructType = StructType(fields)

  final lazy val structTypeString: StructType = StructType(fieldNames.map(s => StructField(s, StringType)))

  final lazy val columns: Array[Column] = columnDefs.map(columnDef => col(columnDef.columnName))
}
