package com.ebay.traffic.chocolate.util

import java.text.SimpleDateFormat
import java.util.Date

import com.ebay.traffic.chocolate.conf.CheckTask
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.xml.XML

/**
  * Created by lxiong1 on 27/11/18.
  */
object XMLUtil {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /** *
    * Read the XML file.
    *
    * @param file
    * @param parameter
    * @return
    */
  def readFile(file: String, parameter: Parameter): List[CheckTask] = {
    var taskList: mutable.ListBuffer[CheckTask] = mutable.ListBuffer[CheckTask]();
    val doc = XML.loadFile(file);
    val tasks = doc \ "task";

    logger.info("read file start by xml");
    for (task <- tasks) {
      val checkTask = CheckTask(task.attribute("name").get.toString(),
        getInputDir(task.attribute("inputDir").get.toString(), parameter.ts, Integer.parseInt(task.attribute("timeDiff").get.toString())),
        getVerifiedTime(parameter.ts),
        Integer.parseInt(task.attribute("timeDiff").get.toString()),
        Integer.parseInt(task.attribute("period").get.toString()),
        task.attribute("dataCountDir").get.toString())
      logger.info(checkTask.jobName);
      logger.info(checkTask.period.toString);
      logger.info(checkTask.ts.toString);
      logger.info(checkTask.inputDir);
      logger.info(checkTask.dataCountDir);
      taskList.+=(checkTask);
    }
    logger.info("read file end by xml");

    return taskList.toList;
  }

  /** *
    * recover the ts to min level.
    *
    * @param ts ms
    * @return
    */
  def getVerifiedTime(ts: Long): Long = {
    val realMin = ts / (1000 * 60);
    return realMin * 1000 * 60;
  }

  /***
    * get the directory which stored the count data.
    *
    * @param jobName
    * @param parameter
    * @return
    */
  def getDataCountDir(jobName: String, parameter: Parameter): String = {
    return parameter.countDataDir + "/" + jobName;
  }

  /***
    * get the input directory.
    *
    * @param rawInputDir
    * @param ts ms
    * @param td hour
    * @return return input directory
    */
  def getInputDir(rawInputDir: String, ts: Long, td: Int): String = {
    val dt = new SimpleDateFormat("yyyy-MM-dd");
    val date = dt.format(new Date(ts - td * 60 * 60 * 1000));
    val inputDir = rawInputDir + date;
    logger.info("inputDir: " + inputDir);
    return inputDir;
  }

}
