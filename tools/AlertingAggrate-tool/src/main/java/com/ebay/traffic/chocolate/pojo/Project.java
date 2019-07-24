package com.ebay.traffic.chocolate.pojo;

import java.util.ArrayList;

public class Project {

  private String name;
  private int id;
  private ArrayList<Metric> list;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public ArrayList<Metric> getList() {
    return list;
  }

  public void setList(ArrayList<Metric> list) {
    this.list = list;
  }

}
