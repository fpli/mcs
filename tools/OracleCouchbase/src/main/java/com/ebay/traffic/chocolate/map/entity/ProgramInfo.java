package com.ebay.traffic.chocolate.map.entity;

import java.io.Serializable;

public class ProgramInfo implements Serializable {
  private String ams_program_id;
  private String ams_country_id;
  private String program_name;

  public String getAms_program_id() {
    return ams_program_id;
  }

  public void setAms_program_id(String ams_program_id) {
    this.ams_program_id = ams_program_id;
  }

  public String getAms_country_id() {
    return ams_country_id;
  }

  public void setAms_country_id(String ams_country_id) {
    this.ams_country_id = ams_country_id;
  }

  public String getProgram_name() {
    return program_name;
  }

  public void setProgram_name(String program_name) {
    this.program_name = program_name;
  }
}
