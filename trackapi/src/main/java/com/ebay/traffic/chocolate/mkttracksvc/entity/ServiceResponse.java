package com.ebay.traffic.chocolate.mkttracksvc.entity;

import java.io.Serializable;

public class ServiceResponse implements Serializable {
  private String message;
  private RotationInfo rotationInfo;

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public RotationInfo getRotationInfo() {
    return rotationInfo;
  }

  public void setRotationInfo(RotationInfo rotationInfo) {
    this.rotationInfo = rotationInfo;
  }
}
