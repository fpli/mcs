package com.ebay.traffic.chocolate.mkttracksvc.dao;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

@Component
public class CouchbaseClient {

  private static final Logger logger = Logger.getLogger(CouchbaseClient.class);

  private MKTTrackSvcConfigBean configBean;

  private Cluster cluster;

  private Bucket rotationBucket;
  private Bucket reportBucket;

  @Autowired
  public CouchbaseClient(MKTTrackSvcConfigBean configBean) {
    this.configBean = configBean;
  }

  @PostConstruct
  private void init() {
    CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
            .mutationTokensEnabled(true)
            .computationPoolSize(configBean.getCbPoolSize())
            .connectTimeout(configBean.getCbConnectionTimeout())
            .queryTimeout(configBean.getCbQueryTimeout())
            .build();

    cluster = CouchbaseCluster.create(environment, configBean.getCbCluster());
    cluster.authenticate(configBean.getCbRotationUser(), configBean.getCbRotationPwd());

    rotationBucket = cluster.openBucket(configBean.getCbBucketRotation(), configBean.getCbBucketTimeout(), TimeUnit.SECONDS);
    reportBucket = cluster.openBucket(configBean.getCbBucketReport(), configBean.getCbBucketTimeout(), TimeUnit.SECONDS);

    logger.info("Initialize Couchbase client successfully.");
  }

  public Bucket getRotationBucket() {
    return rotationBucket;
  }

  public Bucket getReportBucket() {
    return reportBucket;
  }

  @PreDestroy
  private void destroy() {
    if (rotationBucket != null) {
      rotationBucket.close();
    }
    if (reportBucket != null) {
      reportBucket.close();
    }
    cluster.disconnect();

    logger.info("Shutdown Couchbase client successfully.");
  }
}
