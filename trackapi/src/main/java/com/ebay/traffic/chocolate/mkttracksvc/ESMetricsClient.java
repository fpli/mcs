package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;

@Component
@Singleton
public class ESMetricsClient {

  private static final Logger logger = Logger.getLogger(ESMetricsClient.class);
  private static final String METRICS_INDEX_PREFIX = "mkttracksvc-metrics-";
  private ESMetrics esMetrics;
  private MKTTrackSvcConfigBean configBean;

  @Autowired
  public ESMetricsClient(MKTTrackSvcConfigBean configBean) {
    this.configBean = configBean;
  }

  public ESMetrics getEsMetrics() {
    return esMetrics;
  }

  @PostConstruct
  public void iniMetrics(){
    ESMetrics.init(METRICS_INDEX_PREFIX, configBean.getEsEndpoint());
    esMetrics = ESMetrics.getInstance();
    logger.info("Initialize ElasticSearch client successfully.");
  }
}
