package com.ebay.traffic.chocolate.spark

import com.ebay.traffic.chocolate.utils.BaseConf


/**
  * Created by zhofan on 2019/06/11.
  */
object SparkCommonConf extends BaseConf{
  lazy val sparkConf = conf.getConfig("spark")
  lazy val sparkSqlWarehouseDir = sparkConf.getString("sql.warehouse.dir")
  lazy val defaultFS = sparkConf.getString("fs.defaultFS")
}
