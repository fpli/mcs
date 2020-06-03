package com.ebay.app.raptor.chocolate.jdbc.model;

import javax.persistence.*;

/**
 * Created by jialili1 on 11/14/19
 */
@Entity
@Table(name = "thirdparty_whitelist")
public class ThirdpartyWhitelist {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Integer id;

  @Column(name = "type_id")
  private Integer typeId;

  @Column(name = "value")
  private String value;

  public ThirdpartyWhitelist() {
  }

  public ThirdpartyWhitelist(Integer id, Integer typeId, String value) {
    this.id = id;
    this.typeId = typeId;
    this.value = value;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Integer getId() {
    return id;
  }

  public void setTypeId(Integer typeId) {
    this.typeId = typeId;
  }

  public Integer getTypeId() {
    return typeId;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

}
