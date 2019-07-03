package com.ebay.traffic.chocolate.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

/**
  * Created by zhofan on 2019/06/11.
  */
case class ColumnDef(columnName: String,
                     dataType: DataType,
                     index: Int,
                     castFunc: Option[Column => Column] = None,
                     isPrimaryKey: Boolean = false) extends Serializable
