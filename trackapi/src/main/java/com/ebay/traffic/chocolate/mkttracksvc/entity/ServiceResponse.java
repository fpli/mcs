package com.ebay.traffic.chocolate.mkttracksvc.entity;

import java.io.Serializable;
import java.util.List;

public class ServiceResponse implements Serializable {
  private String message;
  private RotationInfo rotation_info;
  private List<RotationInfo> rotation_info_list;

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public RotationInfo getRotation_info() {
    return rotation_info;
  }

  public void setRotation_info(RotationInfo rotation_info) {
    this.rotation_info = rotation_info;
  }

  public List<RotationInfo> getRotation_info_list() {
    return rotation_info_list;
  }

  public void setRotation_info_list(List<RotationInfo> rotation_info_list) {
    this.rotation_info_list = rotation_info_list;
  }
}
