package com.ebay.traffic.chocolate.pojo;

import java.util.ArrayList;

public class Project {

  private String name;
  private String source;
  private ArrayList<Metric> list;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public ArrayList<Metric> getList() {
    return list;
  }

  public void setList(ArrayList<Metric> list) {
    this.list = list;
  }

}
