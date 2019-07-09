package com.ebay.traffic.chocolate.spark

import com.ebay.traffic.chocolate.utils.JobParamUtiliy._


/**
  * Created by zhofan on 2019/06/11.
  */

case class RotationDataParam(defaultFs:String, srcPath: String, destPath: String, partitionNum : Int,
                             sqlFile: String, schema: String, localThread: Int, ds: String)

object RotationDataParam {
  final lazy val defautPartitionNum = 10

  def apply(argMap: Map[String, String]): RotationDataParam = {
    implicit val _argMap = argMap

    RotationDataParam(
      defaultFs = getStringParamOptional("defaultFs", null),
      srcPath = getStringParamOptional("srcPath", null),
      destPath = getStringParamOptional("destPath", null),
      partitionNum = getIntParamOptional("partitionNum", defautPartitionNum),
      sqlFile = getStringParamOptional("sqlFile", null),
      schema = getStringParamOptional("schema", "default"),
      localThread = getIntParamOptional("localThread", -1),
      ds = getStringParamOptional("ds", null)
    )
  }
}
