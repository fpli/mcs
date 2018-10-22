package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import java.io.Serializable;

public class ProgPubMapInfo implements Serializable {
  private String ams_program_id;
  private String ams_publisher_id;
  private String status_enum;
  private String reject_reason_enum;

  public String getAms_program_id() {
    return ams_program_id;
  }

  public void setAms_program_id(String ams_program_id) {
    this.ams_program_id = ams_program_id;
  }

  public String getAms_publisher_id() {
    return ams_publisher_id;
  }

  public void setAms_publisher_id(String ams_publisher_id) {
    this.ams_publisher_id = ams_publisher_id;
  }

  public String getStatus_enum() {
    return status_enum;
  }

  public void setStatus_enum(String status_enum) {
    this.status_enum = status_enum;
  }

  public String getReject_reason_enum() {
    return reject_reason_enum;
  }

  public void setReject_reason_enum(String reject_reason_enum) {
    this.reject_reason_enum = reject_reason_enum;
  }
}
