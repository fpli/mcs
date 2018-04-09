package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.{DataFrame}

/**
  * Created by xiangli4 on 4/8/18.
  */
trait CappingRule {
  def cleanBaseDir()
  def test(dateFiles: DateFiles): DataFrame
  def renameBaseTempFiles(dateFiles: DateFiles)
}
