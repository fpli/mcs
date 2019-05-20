package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import java.io.Serializable;

public class RoiRule implements Serializable {
  private int ams_clk_fltr_type_id;
  private int is_rule_enable;
  private int is_pblshr_advsr_enable_rule;
  private int rule_result;

  public int getAms_clk_fltr_type_id() {
    return ams_clk_fltr_type_id;
  }

  public void setAms_clk_fltr_type_id(int ams_clk_fltr_type_id) {
    this.ams_clk_fltr_type_id = ams_clk_fltr_type_id;
  }

  public int getIs_rule_enable() {
    return is_rule_enable;
  }

  public void setIs_rule_enable(int is_rule_enable) {
    this.is_rule_enable = is_rule_enable;
  }

  public int getIs_pblshr_advsr_enable_rule() {
    return is_pblshr_advsr_enable_rule;
  }

  public void setIs_pblshr_advsr_enable_rule(int is_pblshr_advsr_enable_rule) {
    this.is_pblshr_advsr_enable_rule = is_pblshr_advsr_enable_rule;
  }

  public int getRule_result() {
    return rule_result;
  }

  public void setRule_result(int rule_result) {
    this.rule_result = rule_result;
  }
}
