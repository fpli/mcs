package com.ebay.traffic.chocolate.pojo;

public class RotationAlert {

  private String ClusterName;
  private String tableName;
  private String status;
  private String count;
  private String distinctCount;
  private String diff;

  public String getClusterName() {
    return ClusterName;
  }

  public void setClusterName(String clusterName) {
    ClusterName = clusterName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getCount() {
    return count;
  }

  public void setCount(String count) {
    this.count = count;
  }

  public String getDistinctCount() {
    return distinctCount;
  }

  public void setDistinctCount(String distinctCount) {
    this.distinctCount = distinctCount;
  }

  public String getDiff() {
    return diff;
  }

  public void setDiff(String diff) {
    this.diff = diff;
  }

}
