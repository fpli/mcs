package com.ebay.app.raptor.chocolate.jdbc.model;

import javax.persistence.*;

@Entity
@Table(name = "thirdparty_whitelist")
public class ThirdpartyWhitelist {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Integer id;

  @Column(name = "name")
  private String name;

  @Column(name = "url")
  private String url;

  public ThirdpartyWhitelist() {
  }

  public ThirdpartyWhitelist(Integer id, String name, String url) {
    this.id = id;
    this.name = name;
    this.url = url;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getUrl() {
    return url;
  }
}
