package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.kernel.bean.configuration.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


/**
 * A ConfigBean for properties
 *
 * @author yimeng
 */
@Component
@ConfigurationProperties(prefix = "mkttracksvc")
public class MKTTrackSvcConfigBean extends BaseConfigBean {
  private static BeanPropertyInfo CouchBase_Cluster = createBeanPropertyInfo("cbCluster", "couchbase_cluster", true);
  private static BeanPropertyInfo CouchBase_POOL_SIZE = createBeanPropertyInfo("cbPoolSize", "couchbase_pool_size", true);
  private static BeanPropertyInfo CouchBase_Connection_TImeout = createBeanPropertyInfo("cbConnectionTimeout", "couchbase_connection_timeout", true);
  private static BeanPropertyInfo CouchBase_Query_Timeout = createBeanPropertyInfo("cbQueryTimeout", "couchbase_query_timeout", true);
  private static BeanPropertyInfo CouchBase_Buket_Timeout = createBeanPropertyInfo("cbBucketTimeout", "couchbase_bucket_timeout", true);
  private static BeanPropertyInfo CouchBase_Bucket_Rotation = createBeanPropertyInfo("cbBucketRotation", "couchBase_bucket_rotation", true);
  private static BeanPropertyInfo CouchBase_Bucket_Report = createBeanPropertyInfo("cbBucketReport", "couchBase_bucket_report", true);
  private static BeanPropertyInfo ES_URL = createBeanPropertyInfo("esEndpoint", "elasticsearch_endpoint", true);

  public MKTTrackSvcConfigBean() throws ConfigCategoryCreateException {
    BeanConfigCategoryInfo categoryInfo = new BeanConfigCategoryInfoBuilder()
        .setCategoryId("com.ebay.traffic.chocolate.MKTTrackSvcConfigBean")
        .setAlias("trackapi")
        .setGroup("MktTrackSvc")
        .build();
    // this init method need to be called to bind the ConfigBean instance to a configuration category
    init(categoryInfo, true);
  }


  private Integer cbPoolSize;
  private String cbCluster;
  private Integer cbConnectionTimeout;
  private Integer cbQueryTimeout;
  private Integer cbBucketTimeout;
  private String cbBucketRotation;
  private String cbBucketReport;
  private String cbRotationUser;
  private String cbRotationPwd;
  private String esEndpoint;

  public Integer getCbPoolSize() {
    return cbPoolSize;
  }

  public void setCbPoolSize(Integer cbPoolSize) {
    changeProperty(CouchBase_POOL_SIZE, this.cbPoolSize, cbPoolSize);
  }

  public String getCbCluster() {
    return cbCluster;
  }

  public void setCbCluster(String cbCluster) {
    changeProperty(CouchBase_Cluster, this.cbCluster, cbCluster);
  }

  public Integer getCbConnectionTimeout() {
    return cbConnectionTimeout;
  }

  public void setCbConnectionTimeout(Integer cbConnectionTimeout) {
    changeProperty(CouchBase_Connection_TImeout, this.cbConnectionTimeout, cbConnectionTimeout);
  }

  public Integer getCbQueryTimeout() {
    return cbQueryTimeout;
  }

  public void setCbQueryTimeout(Integer cbQueryTimeout) {
    changeProperty(CouchBase_Query_Timeout, this.cbQueryTimeout, cbQueryTimeout);
  }

  public Integer getCbBucketTimeout() {
    return cbBucketTimeout;
  }

  public void setCbBucketTimeout(Integer cbBucketTimeout) {
    changeProperty(CouchBase_Buket_Timeout, this.cbBucketTimeout, cbBucketTimeout);
  }

  public String getCbBucketRotation() {
    return cbBucketRotation;
  }

  public void setCbBucketRotation(String cbBucketRotation) {
    changeProperty(CouchBase_Bucket_Rotation, this.cbBucketRotation, cbBucketRotation);
  }

  public String getCbBucketReport() {
    return cbBucketReport;
  }

  public void setCbBucketReport(String cbBucketReport) {
    changeProperty(CouchBase_Bucket_Report, this.cbBucketReport, cbBucketReport);
  }

  public String getCbRotationUser() {
    return cbRotationUser;
  }

  public void setCbRotationUser(String cbRotationUser) {
    this.cbRotationUser = cbRotationUser;
  }

  public String getCbRotationPwd() {
    return cbRotationPwd;
  }

  public void setCbRotationPwd(String cbRotationPwd) {
    this.cbRotationPwd = cbRotationPwd;
  }

  public String getEsEndpoint() {
    return esEndpoint;
  }

  public void setEsEndpoint(String esEndpoint) {
    changeProperty(ES_URL, this.esEndpoint, esEndpoint);
  }
}
