package com.ebay.traffic.chocolate.job

import com.ebay.traffic.chocolate.conf.CheckTask
import com.ebay.traffic.chocolate.monitoring.ESMetrics
import com.ebay.traffic.chocolate.task.TaskManger
import com.ebay.traffic.chocolate.util.{Constant, Parameter, XMLUtil}

/**
  * Created by lxiong1 on 27/11/18.
  */
object CheckJob extends App {
  override def main(args: Array[String]) = {
    val params = Parameter(args)

    val job = new CheckJob(params)

    job.run()
    job.metrics.close();
    job.stop()
  }
}

class CheckJob(params: Parameter) extends BaseSparkJob(params.appName, params.mode) {

  @transient lazy val metrics: ESMetrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(Constant.ES_METRIC_PRE, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  @transient lazy val listTask: List[CheckTask] = {
    if (params.mode.indexOf("local") == 0) {
      logger.info("get listTask for test.");
      List(CheckTask("testJob", params.taskFile, 1543477200000l, 0, 5, params.countDataDir));
    } else {
      logger.info("get listTask.");
      XMLUtil.readFile(params.taskFile, params);
    }
  }

  /***
    * the entrance of the checkJob.
    */
  def run(): Unit = {

    // step 1: read hdfs,
    // step 2: run task,
    // step 3: save the data to hdfs,
    // step 4: send data to grafana
    logger.info("check job start.");
    TaskManger.runTasks(listTask, metrics, spark);
    logger.info("check job end.");
  }

}
