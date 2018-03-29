package com.ebay.traffic.chocolate.sparknrt.meta

import java.io.ByteArrayOutputStream
import java.io.OutputStream

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
  * Created by yliu29 on 3/23/18.
  */
class Metadata(workDir: String) {

  lazy val DEDUPE_COMP_META = workDir + "/meta/dedupe_comp.meta"
  lazy val DEDUPE_OUTPUT_META_DIR = workDir + "/meta/output/"
  lazy val DEDUPE_OUTPUT_META_PREFIX =  "dedupe_output_"

  lazy val jsonMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(DefaultScalaModule)

  lazy val hadoopConf = {
    new Configuration()
  }

  lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  /**
    * Dedupe output meta
    */
  def readDedupeOutputMeta(): Array[(String, Map[String, Array[String]])] = {
    val status = fs.listStatus(new Path(DEDUPE_OUTPUT_META_DIR))
    status.map(s => s.getPath).filter(path => path.getName.startsWith(DEDUPE_OUTPUT_META_PREFIX))
      .map(path => {
        val file = path.toString
        val content = readFileContent(file)
        val metaFiles = readMetaFiles(content).metaFiles.map(dateFiles => {
          (dateFiles.date, dateFiles.files)
        }).toMap
        (file, metaFiles)
      })
  }

  def writeDedupeOutputMeta(dedupeOutputMeta: MetaFiles) = {
    val time = System.currentTimeMillis()
    writeMetaFiles(dedupeOutputMeta, DEDUPE_OUTPUT_META_DIR + DEDUPE_OUTPUT_META_PREFIX + time + ".meta")
  }

  def deleteDedupeOutputMeta(metaFile: String) = {
    fs.delete(new Path(metaFile), true)
  }

  /**
    * Dedupe comparison meta
    */
  def readDedupeCompMeta(): Map[String, Array[String]] = {
    val content = readFileContent(DEDUPE_COMP_META)
    if (content != null) {
      readMetaFiles(content).metaFiles.map(dateFiles => {
        (dateFiles.date, dateFiles.files)
      }).toMap
    } else {
      null
    }
  }

  def writeDedupeCompMeta(dedupeCompMeta: MetaFiles) = {
    writeMetaFiles(dedupeCompMeta, DEDUPE_COMP_META)
  }

  private def readFileContent(file: String): String = {
    val path = new Path(file)
    if (fs.exists(path)) {
      val in = fs.open(path)
      val out = new ByteArrayOutputStream()
      val buffer = new Array[Byte](1024)
      var n = 0
      while(n > -1) {
        n = in.read(buffer)
        if(n > 0) {
          out.write(buffer, 0, n)
        }
      }
      in.close()
      out.toString
    } else {
      null
    }
  }

  private def readMetaFiles(content: String): MetaFiles = {
    jsonMapper.readValue(content, classOf[MetaFiles])
  }

  private def writeMetaFiles(metaFiles: MetaFiles, file: String) = {
    val out: OutputStream = fs.create(new Path(file), true)
    jsonMapper.writeValue(out, metaFiles)
    out.close()
  }
}

object Metadata {

  def apply(workDir: String): Metadata = {
    new Metadata(workDir)
  }

}

case class MetaFiles(val metaFiles: Array[DateFiles])

case class DateFiles(val date: String, val files: Array[String])
