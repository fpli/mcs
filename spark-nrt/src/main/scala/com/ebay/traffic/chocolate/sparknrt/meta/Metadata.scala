package com.ebay.traffic.chocolate.sparknrt.meta

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.net.URI

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
  * Created by yliu29 on 3/23/18.
  */
class Metadata(workDir: String, channel: String, usage: MetadataEnum.Value) {

  lazy val DEDUPE_COMP_META = workDir + "/meta/" + channel + "/dedupe_comp.meta"
  lazy val DEDUPE_OUTPUT_META_DIR = workDir + "/meta/" + channel + "/output/" + usage + "/"
  lazy val DEDUPE_OUTPUT_META_PREFIX =  usage + "_output_"

  lazy val jsonMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(DefaultScalaModule)

  lazy val hadoopConf = {
    new Configuration()
  }

  lazy val fs = {
    val fs = FileSystem.get(URI.create(workDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  /**
    * Dedupe output metaaddShutdownHook
    */
  def readDedupeOutputMeta(suffix: String = ""): Array[(String, Map[String, Array[String]])] = {
    val status = fs.listStatus(new Path(DEDUPE_OUTPUT_META_DIR))
    status
      .map(s => s.getPath)
      .filter(path => {
        if (StringUtils.isEmpty(suffix)) {
          path.getName.startsWith(DEDUPE_OUTPUT_META_PREFIX) && path.getName.endsWith(".meta")
        } else {
          path.getName.startsWith(DEDUPE_OUTPUT_META_PREFIX) && path.getName.endsWith(suffix)
        }
      })
      .map(path => {
        val file = path.toString
        val content = readFileContent(file)
        val metaFiles = readMetaFiles(content).metaFiles.map(dateFiles => {
          (dateFiles.date, dateFiles.files)
        }).toMap
        (file, metaFiles)
      })
      .sortBy(e => {
        var name = e._1.substring(e._1.lastIndexOf("/") + 1)
        if (StringUtils.isNotEmpty(suffix)) {
          name = name.substring(0, name.lastIndexOf(suffix))
        }
        name.substring(DEDUPE_OUTPUT_META_PREFIX.length, name.lastIndexOf(".")).toLong
      })
  }

  def writeDedupeOutputMeta(dedupeOutputMeta: MetaFiles, suffixArray: Array[String] = Array()):Unit = {
    val time = System.currentTimeMillis()
    // Ensure a default meta without suffix is written out.
    writeMetaFiles(dedupeOutputMeta, DEDUPE_OUTPUT_META_DIR + DEDUPE_OUTPUT_META_PREFIX + time + ".meta")
    suffixArray.foreach(suffix => {
      writeMetaFiles(dedupeOutputMeta, DEDUPE_OUTPUT_META_DIR + DEDUPE_OUTPUT_META_PREFIX + time + ".meta" + suffix)
    })
  }

  def writeOutputMeta(outputMeta: MetaFiles,outputMetaTmpDir: String, outputMetaDir: String, usage: String, suffixArray: Array[String] = Array()):Unit = {
    val tmpPath = new Path(outputMetaTmpDir)
    if (fs.exists(tmpPath) && fs.listStatus(tmpPath).nonEmpty) {
      fs.delete(tmpPath, true)
    }
    if (!fs.exists(tmpPath))
      fs.mkdirs(tmpPath)
    val time = System.currentTimeMillis()
    suffixArray.foreach(suffix => {
      writeMetaFiles(outputMeta, outputMetaTmpDir + usage + "_output_" + time + ".meta" + suffix)
    })
    renameMetaFiles(outputMetaTmpDir, outputMetaDir, time, usage, suffixArray)
  }

  def renameMetaFiles(src: String, dest: String, time: Long, usage: String, suffixArray: Array[String] = Array()): Unit = {
    suffixArray.foreach(
      suffix => {
        val srcFile = new Path(src + usage + "_output_" + time + ".meta" + suffix)
        val destFile = new Path(dest + usage + "_output_" + time + ".meta" + suffix)
        fs.rename(srcFile, destFile)
      }
    )
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

  def apply(workDir: String, channel: String, usage: MetadataEnum.Value): Metadata = {
    new Metadata(workDir, channel, usage)
  }

}

// Enum of Metadata usages
object MetadataEnum extends Enumeration {

  val unknown = Value(-1)
  val dedupe = Value(0)
  val capping = Value(1)
  val epnnrt_click = Value(2)
  val epnnrt_imp = Value(3)
  val epnnrt_scp_click = Value(4)
  val epnnrt_scp_imp = Value(5)
  val imkDump = Value(6)

  def convertToMetadataEnum(value: String): MetadataEnum.Value = {
    if (value == dedupe.toString) {
      dedupe
    } else if (value == capping.toString) {
      capping
    } else if (value == epnnrt_click.toString) {
      epnnrt_click
    } else if (value == epnnrt_imp.toString) {
      epnnrt_imp
    } else if (value == epnnrt_scp_click.toString) {
      epnnrt_scp_click
    } else if (value == epnnrt_scp_imp.toString) {
      epnnrt_scp_imp
    } else if (value == imkDump.toString) {
      imkDump
    } else {
      unknown
    }
  }
}

case class MetaFiles(val metaFiles: Array[DateFiles])

case class DateFiles(val date: String, val files: Array[String])
