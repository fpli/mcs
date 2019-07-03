package com.ebay.traffic.chocolate.spark

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.commons.io.IOUtils


/**
  * Created by zhofan on 2019/06/11.
  */
object TestFileUtility {
  def createDir(filePath: String) = {
    val file = new File(filePath)
    if (!file.exists)
      file.mkdirs()
    else if (!file.isDirectory) {
      file.delete()
      file.mkdirs()
    }
    else emptyDir(file)
  }

  def touchFile(filePath: String) = {
    val file = new File(filePath)

    file.createNewFile()
  }

  def emptyDir(dir: File): Unit =
    if (dir.isDirectory) {
      dir.listFiles().foreach(f => {
        if (f.isDirectory) {
          emptyDir(f)
          f.delete()
        }
        else f.delete()
      })
    }

  def delFile(filePath: String) = {
    val file = new File(filePath)

    if (file.exists) {
      if (file.isDirectory) emptyDir(file)
      file.delete()
    }
  }

  def copyResourceFiles(destDirPath: String, resourceDirPath: String)(resourceFiles: String*): Unit = {
    resourceFiles.foreach(s => {
      val is = getClass.getResourceAsStream(s"$resourceDirPath/$s")
      val os = new FileOutputStream(s"$destDirPath/$s")

      IOUtils.copy(is, os)
      is.close()
      os.close()
    })
  }

  def copyResourceDirs(destDirPath: String, resourceParentDirPath: String)(resourceDirs: String*): Unit = {
    createDir(destDirPath)
    resourceDirs.foreach(s => {
      val resourceDirPath = getClass.getResource(s"$resourceParentDirPath/$s/")
      createDir(s"$destDirPath/$s")
      copyDir(s"$destDirPath/$s", resourceDirPath.getPath)
    })
  }

  def copyDir(destDirPath: String, sourceDirPath: String): Unit = {
    val sourceDir = new File(sourceDirPath)
    if (sourceDir.isDirectory) {
      sourceDir.listFiles().foreach(f => {
        val destPath = s"$destDirPath/${f.getName}"
        if (f.isDirectory) {
          val destDir = new File(destPath)
          destDir.mkdir()
          copyDir(destPath, f.getAbsolutePath)
        }
        else {
          val is = new FileInputStream(f)
          val os = new FileOutputStream(destPath)

          IOUtils.copy(is, os)
          is.close()
          os.close()
        }
      })
    }
  }
}
