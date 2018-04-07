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
  private static BeanPropertyInfo CouchBase_Cluster = createBeanPropertyInfo("cbCluster", "CouchBase_Cluster", true);
  private static BeanPropertyInfo CouchBase_Connection_TImeout = createBeanPropertyInfo("cbConnectionTimeout", "CouchBase_Connection_TImeout", true);
  private static BeanPropertyInfo CouchBase_Query_Timeout = createBeanPropertyInfo("cbQueryTimeout", "CouchBase_Query_Timeout", true);
  private static BeanPropertyInfo CouchBase_Buket_Timeout = createBeanPropertyInfo("cbBucketTimeout", "CouchBase_Buket_Timeout", true);
  private static BeanPropertyInfo CouchBase_Bucket_Rotation = createBeanPropertyInfo("cbBucketRotation", "CouchBase_Bucket_Rotation", true);


  public MKTTrackSvcConfigBean() throws ConfigCategoryCreateException {
    BeanConfigCategoryInfo categoryInfo = new BeanConfigCategoryInfoBuilder()
        .setCategoryId("com.ebay.traffic.chocolate.MKTTrackSvcConfigBean")
        .setAlias("trackapi")
        .setGroup("MktTrackSvc")
        .build();
    // this init method need to be called to bind the ConfigBean instance to a configuration category
    init(categoryInfo, true);
  }


  private String cbCluster;
  private Integer cbConnectionTimeout;
  private Integer cbQueryTimeout;
  private Integer cbBucketTimeout;
  private String cbBucketRotation;
  private String cbRotationUser;
  private String cbRotationPwd;

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
}
