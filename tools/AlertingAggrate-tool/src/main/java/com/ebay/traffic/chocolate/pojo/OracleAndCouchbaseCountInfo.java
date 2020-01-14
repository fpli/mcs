package com.ebay.traffic.chocolate.pojo;

public class OracleAndCouchbaseCountInfo {

  private String tableName;
  private String onedayCountInOracle;
  private String onedayCountInCouchbase;
  private String alldayCountInOracle;
  private String alldayCountInCouchbase;
  private String tableType;

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getOnedayCountInOracle() {
    return onedayCountInOracle;
  }

  public void setOnedayCountInOracle(String onedayCountInOracle) {
    this.onedayCountInOracle = onedayCountInOracle;
  }

  public String getOnedayCountInCouchbase() {
    return onedayCountInCouchbase;
  }

  public void setOnedayCountInCouchbase(String onedayCountInCouchbase) {
    this.onedayCountInCouchbase = onedayCountInCouchbase;
  }

  public String getAlldayCountInOracle() {
    return alldayCountInOracle;
  }

  public void setAlldayCountInOracle(String alldayCountInOracle) {
    this.alldayCountInOracle = alldayCountInOracle;
  }

  public String getAlldayCountInCouchbase() {
    return alldayCountInCouchbase;
  }

  public void setAlldayCountInCouchbase(String alldayCountInCouchbase) {
    this.alldayCountInCouchbase = alldayCountInCouchbase;
  }

  public String getTableType() {
    return tableType;
  }

  public void setTableType(String tableType) {
    this.tableType = tableType;
  }

}
