package com.ebay.traffic.chocolate.cappingrules.dto;

/**
 * Created by yimeng on 11/14/17.
 */
public class SNIDCapperResult implements CapperIdentity{
  private long snapshotId;
  private Boolean isImpressed;
  //private long impSnapshotId;
  
  public long getSnapshotId() {
    return snapshotId;
  }
  
  public void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }
  
  public Boolean getImpressed() {
    return isImpressed;
  }
  
  public void setImpressed(Boolean impressed) {
    isImpressed = impressed;
  }

//  public long getImpSnapshotId() {
//    return impSnapshotId;
//  }
//
//  public void setImpSnapshotId(long impSnapshotId) {
//    this.impSnapshotId = impSnapshotId;
//  }
}
