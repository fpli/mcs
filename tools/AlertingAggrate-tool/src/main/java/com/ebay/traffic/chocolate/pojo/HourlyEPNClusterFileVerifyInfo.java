package com.ebay.traffic.chocolate.pojo;

public class HourlyEPNClusterFileVerifyInfo {


  private String clusterName;
  private String newestDoneFile;
  private String newestIdentifiedFileSequence;
  private String allIdentifiedFileNum;
  private String diff;
  private String status;

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getNewestDoneFile() {
    return newestDoneFile;
  }

  public void setNewestDoneFile(String newestDoneFile) {
    this.newestDoneFile = newestDoneFile;
  }

  public String getNewestIdentifiedFileSequence() {
    return newestIdentifiedFileSequence;
  }

  public void setNewestIdentifiedFileSequence(String newestIdentifiedFileSequence) {
    this.newestIdentifiedFileSequence = newestIdentifiedFileSequence;
  }

  public String getAllIdentifiedFileNum() {
    return allIdentifiedFileNum;
  }

  public void setAllIdentifiedFileNum(String allIdentifiedFileNum) {
    this.allIdentifiedFileNum = allIdentifiedFileNum;
  }

  public String getDiff() {
    return diff;
  }

  public void setDiff(String diff) {
    this.diff = diff;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

}
