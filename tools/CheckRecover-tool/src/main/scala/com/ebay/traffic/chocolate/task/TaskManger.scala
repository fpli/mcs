package com.ebay.traffic.chocolate.task

import java.util.Date

import com.ebay.traffic.chocolate.conf.CheckTask
import com.ebay.traffic.chocolate.hdfs.{FileSystemReader, FileSystemWriter}
import com.ebay.traffic.chocolate.monitoring.ESMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created by lxiong1 on 27/11/18.
  */
object TaskManger {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Run the all task in the current time.
    *
    * @param tasks
    */
  def runTasks(tasks: List[CheckTask], esMetrics: ESMetrics, spark: SparkSession) = {
    for (task <- tasks) {
      runTask(task, esMetrics, spark)
    }
  }

  /**
    * Run the current task in the current time.
    *
    * @param checkTask
    */
  def runTask(checkTask: CheckTask, esMetrics: ESMetrics, spark: SparkSession) = {
    if (isRunnableTask(checkTask) && checkTask.period != 0) {
      //for many time one day
      //step 1.read last count from hdfs (c1);
      val lastCount = getLastCount(FileSystemReader.read(checkTask.dataCountDir, checkTask.dataCountURI, spark), checkTask)
      logger.info("name: " + checkTask.jobName + "-----lastCount: " + lastCount)

      //step 2.count the current file (c2);
      val currentCount = FileSystemReader.getFileNum(checkTask.inputDir, checkTask.inputURI)
      logger.info("name: " + checkTask.jobName + "-----currentCount: " + currentCount)

      //step 3. save to hdfs;
      FileSystemWriter.write(checkTask.dataCountDir, checkTask.dataCountURI, new CountData(checkTask.jobName, checkTask.ts, currentCount), spark)

      //step 4.send the current files count to ES ( = c2- c1);
      esMetrics.trace(checkTask.jobName, getCount(currentCount, lastCount, checkTask.ts), checkTask.ts)
      logger.info("name: " + checkTask.jobName + "-----esMetrics send successfully")
    }else if (isRunnableTask(checkTask) && checkTask.period == 0){
      //for once one day
      //step 1.count the current file (c2);
      val currentCount = FileSystemReader.getFileNum(checkTask.inputDir, checkTask.inputURI)
      logger.info("name: " + checkTask.jobName + "-----currentCount: " + currentCount)

      //step 2.send the current files count to ES ( = c2);
      esMetrics.trace(checkTask.jobName, currentCount, checkTask.ts)
      logger.info("name: " + checkTask.jobName + "-----esMetrics send successfully")
    }
  }

  /**
    * Check the status of the current job.
    *
    * @param checkTask
    * @return return true if the ob is the runnable.
    */
  def isRunnableTask(checkTask: CheckTask): Boolean = {
    if (checkTask.period != 0 && (new Date(checkTask.ts).getMinutes() % checkTask.period) == 0) {
      return true
    } else if (checkTask.period == 0 && new Date(checkTask.ts).getHours() == 0 && new Date(checkTask.ts).getMinutes() == 0){
      return true
    }else{
      return false
    }
  }

  /**
    * Get the generated file count of the last job
    *
    * @param data
    * @param checkTask
    * @return return the count of the file.
    */
  def getLastCount(data: DataFrame, checkTask: CheckTask): Int = {
    if (data != null) {
      val lastCount = data.filter(col => {
        col.get(1).toString.equals((checkTask.ts - checkTask.period * 60 * 1000).toString)
      })

      if (lastCount.count() > 0) {
        return Integer.parseInt(lastCount.take(1)(0).get(2).toString)
      } else {
        return 0
      }
    }
    return 0
  }

  /**
    * Get the count of the file in the current day.
    *
    * @param currentCount
    * @param lastCount
    * @param ts
    * @return
    */
  def getCount(currentCount: Int, lastCount: Int, ts: Long): Int = {
    if (isWholeDay(ts)) {
      return currentCount
    } else {
      return currentCount - lastCount
    }
  }

  /**
    * Judge the ts is Whole day or not
    *
    * @param ts
    * @return return true if the ts is whole day.
    */
  def isWholeDay(ts: Long): Boolean = {
    val time = new Date(ts)
    if (time.getHours == 0 && time.getMinutes == 0 && time.getSeconds == 0) {
      return true
    } else {
      return false
    }
  }


}
