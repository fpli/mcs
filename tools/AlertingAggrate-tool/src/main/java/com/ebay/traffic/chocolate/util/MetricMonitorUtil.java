package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.config.MetricConfig;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkJob;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkProject;
import com.ebay.traffic.chocolate.pojo.metric.monitor.MetricResult;
import com.ebay.traffic.chocolate.pojo.metric.monitor.SherlockResponse;
import com.ebay.traffic.chocolate.util.metric.MetricEnum;
import com.ebay.traffic.chocolate.util.metric.MetricUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class MetricMonitorUtil {
  private static final Logger logger = LoggerFactory.getLogger(MetricMonitorUtil.class);
  private static final MetricConfig metricConfig = MetricConfig.getInstance();
  
  private static ObjectMapper objectMapper = new ObjectMapper();
  
  public static void dealMetricMonitorInfo() throws InterruptedException {
    if(metricConfig.getFlinkProjectList() != null && metricConfig.getFlinkProjectList().size() > 0) {
      dealFlinkMonitorInfo(metricConfig.getFlinkProjectList());
    }
    return ;
  }
  
  public static void dealFlinkMonitorInfo(List<FlinkProject> flinkProjectList) throws InterruptedException {
    if(flinkProjectList != null && flinkProjectList.size() > 0) {
      for(FlinkProject flinkProject : flinkProjectList) {
        logger.info("deal flink project: " + flinkProject.getProjectName());
        if(flinkProject.getFlinkJobs() != null && flinkProject.getFlinkJobs().size() > 0) {
            //deal flink job
            enrichFlinkMonitorInfoFromSherlock(flinkProject);
        }
      }
    }
    return ;
  }
  
  static class MetricCallable implements Callable<Boolean> {
    private final Callable<Boolean> callable;
    private final String threadName;
    
    MetricCallable(Callable<Boolean> callable, String threadName) {
      this.callable = callable;
      this.threadName = threadName;
    }
    
    @Override
    public Boolean call() throws Exception {
      Thread.currentThread().setName(threadName);
      try {
        return callable.call();
      } finally {
        // Resetting the thread name to its original value, if needed
        // Thread.currentThread().setName(originalThreadName);
      }
    }
  }
  public static void enrichFlinkMonitorInfoFromSherlock(FlinkProject flinkProject) throws InterruptedException {
    String token = MetricUtil.getToken();
    if(token == null){
      throw new InterruptedException("get token failed");
    }
    try {
      ExecutorService executorService = Executors.newFixedThreadPool(6);
      List<Callable<Boolean>> tasks = new ArrayList<>();
      tasks.add(new MetricCallable(() -> enrichFlinkJobMetric(flinkProject, token, MetricEnum.FLINK_JOB_UPTIME), MetricEnum.FLINK_JOB_UPTIME.name()));
      tasks.add(new MetricCallable(() -> enrichFlinkJobMetric(flinkProject, token, MetricEnum.FLINK_JOB_DOWNTIME), MetricEnum.FLINK_JOB_DOWNTIME.name()));
      tasks.add(new MetricCallable(() -> enrichFlinkJobMetric(flinkProject, token, MetricEnum.FLINK_JOB_LAST_CHECKPOINT_DURATION), MetricEnum.FLINK_JOB_LAST_CHECKPOINT_DURATION.name()));
      tasks.add(new MetricCallable(() -> enrichFlinkJobMetric(flinkProject, token, MetricEnum.FLINK_JOB_NUMBER_OF_COMPLETED_CHECKPOINTS), MetricEnum.FLINK_JOB_NUMBER_OF_COMPLETED_CHECKPOINTS.name()));
      tasks.add(new MetricCallable(() -> enrichFlinkJobMetric(flinkProject, token, MetricEnum.FLINK_JOB_NUMBER_OF_FAILED_CHECKPOINTS), MetricEnum.FLINK_JOB_NUMBER_OF_FAILED_CHECKPOINTS.name()));
      tasks.add(new MetricCallable(() -> enrichConsumerLagMetric(flinkProject, token, MetricEnum.FLINK_JOB_CONSUMER_LAG), MetricEnum.FLINK_JOB_CONSUMER_LAG.name()));
      List<Future<Boolean>> results = executorService.invokeAll(tasks);
      for (Future<Boolean> result : results) {
        logger.info("Enrich result: " + result.get());
      }
      executorService.shutdown();
    }catch (Exception e) {
      logger.error("enrichFlinkMonitorInfoFromSherlock error", e);
      e.printStackTrace();
    }
  }

  public static boolean enrichFlinkJobMetric(FlinkProject flinkProject, String token, MetricEnum metricEnum) {
    String queryMetric = "";
    long step = 0;
    long endTime = System.currentTimeMillis() / 1000;
    switch (metricEnum){
      case FLINK_JOB_UPTIME:
        queryMetric = String.format(metricConfig.getUptimeMetricTemplate(), flinkProject.getNamespace(), flinkProject.getApplicationNames(),flinkProject.getJobNames());
        step = metricConfig.getUptimeMetricStep();
        break;
      case FLINK_JOB_DOWNTIME:
        queryMetric = String.format(metricConfig.getDowntimeMetricTemplate(), flinkProject.getNamespace(), flinkProject.getApplicationNames(),flinkProject.getJobNames());
        step = metricConfig.getDowntimeMetricStep();
        break;
      case FLINK_JOB_LAST_CHECKPOINT_DURATION:
        queryMetric = String.format(metricConfig.getLastCheckpointDurationMetricTemplate(), flinkProject.getNamespace(), flinkProject.getApplicationNames(),flinkProject.getJobNames());
        step = metricConfig.getLastCheckpointDurationMetricStep();
        break;
      case FLINK_JOB_NUMBER_OF_COMPLETED_CHECKPOINTS:
        queryMetric = String.format(metricConfig.getNumberOfCompletedCheckpointsMetricTemplate(), flinkProject.getNamespace(), flinkProject.getApplicationNames(),flinkProject.getJobNames());
        step = metricConfig.getNumberOfCompletedCheckpointsMetricStep();
        break;
      case FLINK_JOB_NUMBER_OF_FAILED_CHECKPOINTS:
        queryMetric = String.format(metricConfig.getNumberOfFailedCheckpointsMetricTemplate(), flinkProject.getNamespace(), flinkProject.getApplicationNames(),flinkProject.getJobNames());
        step = metricConfig.getNumberOfFailedCheckpointsMetricStep();
        break;
      case FLINK_JOB_CONSUMER_LAG:
        queryMetric = String.format(metricConfig.getConsumerlagMetricTemplate(), flinkProject.getNamespace(), flinkProject.getApplicationNames(),flinkProject.getJobNames());
        step = metricConfig.getConsumerlagMetricStep();
        break;
      default:
        break;
    }
    try {
      long startTime = endTime - step * 60;
      String res = MetricUtil.getSherlockMetric(queryMetric, startTime, endTime, (long) (5*60), token);
      //String res = "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"applicationName\":\"utp-event-imk-sink-13\",\"job_name\":\"UTPImkSinkApp\"},\"values\":[[1708495209,\"119597952\"],[1708495509,\"119897983\"],[1708495809,\"120198014\"],[1708496109,\"120498045\"],[1708496409,\"120798075\"],[1708496709,\"121098107\"],[1708497009,\"121398137\"]]},{\"metric\":{\"applicationName\":\"utp-imk-transform-13\",\"job_name\":\"UTPImkTransformApp\"},\"values\":[[1708495209,\"383988653\"],[1708495509,\"384288686\"],[1708495809,\"384588720\"],[1708496109,\"384888765\"],[1708496409,\"385188797\"],[1708496709,\"385488831\"],[1708497009,\"385788865\"]]}]}}";
      List<MetricResult> metricResultList = parseSherlockResult(res);
      for (FlinkJob flinkJob : flinkProject.getFlinkJobs()) {
        for(MetricResult metricResult : metricResultList) {
          if(flinkJob.getApplicationName().equals(metricResult.getApplicationName()) && flinkJob.getJobName().equals(metricResult.getJobName())
           && metricResult.getMetricValues() != null && metricResult.getMetricValues().size() > 0) {
            List<Long> metricValues = metricResult.getMetricValues();
            switch (metricEnum){
              case FLINK_JOB_UPTIME:
                flinkJob.setUpTime(metricValues.get(metricValues.size() - 1));
                break;
              case FLINK_JOB_DOWNTIME:
                flinkJob.setDownTime(metricValues.get(metricValues.size() - 1));
                break;
              case FLINK_JOB_LAST_CHECKPOINT_DURATION:
                flinkJob.setLastCheckpointDuration(metricValues.get(metricValues.size() - 1));
                break;
              case FLINK_JOB_NUMBER_OF_COMPLETED_CHECKPOINTS:
                if(metricValues.get(metricValues.size() - 1) != null && metricValues.get(0) != null) {
                  if(metricValues.get(metricValues.size() - 1) - metricValues.get(0) >= 0) {
                    flinkJob.setNumberOfCompletedCheckpoint(metricValues.get(metricValues.size() - 1) - metricValues.get(0));
                  }else {
                    flinkJob.setNumberOfCompletedCheckpoint(metricValues.get(metricValues.size() - 1));
                  }
                }
                break;
              case FLINK_JOB_NUMBER_OF_FAILED_CHECKPOINTS:
                if(metricValues.get(metricValues.size() - 1) != null && metricValues.get(0) != null) {
                  if (metricValues.get(metricValues.size() - 1) - metricValues.get(0) >= 0) {
                    flinkJob.setNumberOfFailedCheckpoint(metricValues.get(metricValues.size() - 1) - metricValues.get(0));
                  } else {
                    flinkJob.setNumberOfFailedCheckpoint(metricValues.get(metricValues.size() - 1));
                  }
                }
                break;
              case FLINK_JOB_CONSUMER_LAG:
                break;
              default:
                break;
            }
          }
        }
      }
      return true;
    }catch (Exception e) {
      logger.error("enrichUptimeMetric error", e);
      e.printStackTrace();
      return false;
    }
  }
  
  public static boolean enrichConsumerLagMetric(FlinkProject flinkProject, String token, MetricEnum metricEnum) {
    String queryMetric = "";
    long step = 0;
    long endTime = System.currentTimeMillis() / 1000;
    switch (metricEnum){
      case FLINK_JOB_CONSUMER_LAG:
        queryMetric = String.format(metricConfig.getConsumerlagMetricTemplate(), flinkProject.getZones(), flinkProject.getGroupIds(),flinkProject.getSourceTopics());
        step = metricConfig.getConsumerlagMetricStep();
        break;
      default:
        break;
    }
    try {
      long startTime = endTime - step * 60;
      String res = MetricUtil.getSherlockMetric(queryMetric, startTime, endTime, (long) (60*10), token);
      //String res = "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"groupId\":\"utp-imk-sink\",\"topic\":\"marketing.tracking.events.imk\",\"zone\":\"lvs\"},\"values\":[[1708495209,\"14210\"],[1708495509,\"97825\"],[1708495809,\"61616\"],[1708496109,\"13408\"],[1708496409,\"108724\"],[1708496709,\"59417\"],[1708497009,\"14169\"]]},{\"metric\":{\"groupId\":\"utp-to-imk\",\"topic\":\"marketing.tracking.events.total\",\"zone\":\"lvs\"},\"values\":[[1708495209,\"1075132\"],[1708495509,\"83825\"],[1708495809,\"2428755\"],[1708496109,\"1092889\"],[1708496409,\"92797\"],[1708496709,\"2339378\"],[1708497009,\"1300781\"]]}]}}";
      List<MetricResult> metricResultList = parseSherlockResult(res);
      for (FlinkJob flinkJob : flinkProject.getFlinkJobs()) {
        for(MetricResult metricResult : metricResultList) {
          if(flinkJob.getZone().equals(metricResult.getZone()) && flinkJob.getGroupId().equals(metricResult.getGroupId())
                  && flinkJob.getSourceTopic().equals(metricResult.getSourceTopic())
                  && metricResult.getMetricValues() != null && metricResult.getMetricValues().size() > 0) {
            List<Long> metricValues = metricResult.getMetricValues();
            switch (metricEnum){
              case FLINK_JOB_CONSUMER_LAG:
                flinkJob.setConsumerLags(metricValues);
                break;
              default:
                break;
            }
          }
        }
      }
      return true;
    }catch (Exception e) {
      logger.error("enrichUptimeMetric error", e);
      e.printStackTrace();
      return false;
    }
  }
  
  public static List<MetricResult> parseSherlockResult(String res) {
    //deal res
   List<MetricResult> metricResultList= new ArrayList<>();
    if(res == null) {
      return metricResultList;
    }
    try {
      SherlockResponse sherlockResponse = objectMapper.readValue(res, SherlockResponse.class);
      if("success".equals(sherlockResponse.getStatus()) && sherlockResponse.getData() != null && sherlockResponse.getData().getResult() != null
              && sherlockResponse.getData().getResult().size() > 0) {
        List<SherlockResponse.Result> results = sherlockResponse.getData().getResult();
        for(SherlockResponse.Result result : results) {
          SherlockResponse.Metric metric = result.getMetric();
          List<List<Object>> values = result.getValues();
          if(metric != null){
            MetricResult metricResult = new MetricResult();
            metricResult.setApplicationName(metric.getApplicationName());
            metricResult.setJobName(metric.getJobName());
            metricResult.setGroupId(metric.getGroupId());
            metricResult.setZone(metric.getZone());
            metricResult.setSourceTopic(metric.getTopic());
            metricResult.setValues(values);
            if(values != null && values.size() > 0) {
              List<Long> metricValues = new ArrayList<>();
              for(List<Object> value : values) {
                if(value.size() >= 1) {
                  metricValues.add(Long.parseLong((String) value.get(1)));
                }
              }
              metricResult.setMetricValues(metricValues);
            }
            metricResultList.add(metricResult);
          }
        }
      }
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return metricResultList;
  }
  
}
