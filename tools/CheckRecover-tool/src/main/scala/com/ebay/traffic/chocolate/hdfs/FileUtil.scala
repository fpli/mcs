package com.ebay.traffic.chocolate.hdfs

import com.ebay.traffic.chocolate.hdfs.FileSystemReader.fs
import org.apache.hadoop.fs.Path

object FileUtil {

  def isExistFile(file: String): Boolean = {
    val fsStatus = fs.listStatus(new Path(file));
    val files = fsStatus.filter(status => status.getPath.getName != "_SUCCESS");
    if (fsStatus.size > 0) {
      return true;
    } else {
      return false;
    }
  }

}
