/*
 * service-tracking-events
 * marketing tracking compoent to receive marketing events
 *
 * OpenAPI spec version: 1.0.0
 *
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */

package com.ebay.app.raptor.chocolate.gen.model;

import java.util.Objects;
import java.util.Arrays;
import java.io.Serializable;
import io.swagger.annotations.*;

import io.swagger.v3.oas.annotations.media.Schema;
import com.fasterxml.jackson.annotation.*;

/**
 * Json of soj tags
 */

@ApiModel(description = "Json of soj tags")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2020-05-21T17:31:11.377+08:00[Asia/Shanghai]")
@JsonPropertyOrder({ "pageId","nid","ntype","userName","mc3id","pnact" })
@JsonIgnoreProperties(ignoreUnknown = true)


public class EventPayload implements Serializable {

  private static final long serialVersionUID = 1L;



  @JsonProperty("pageId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer pageId = null;
  @JsonProperty("nid")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String nid = null;
  @JsonProperty("ntype")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String ntype = null;
  @JsonProperty("user_name")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String userName = null;
  @JsonProperty("mc3id")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String mc3id = null;
  @JsonProperty("pnact")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String pnact = null;

  /**
   * page id, it&#x27;s mandotary for notification
   * @return pageId
   **/
  @ApiModelProperty(example = "2054081", value = "page id, it's mandotary for notification")
  public Integer getPageId() {
    return pageId;
  }

  public void setPageId(Integer pageId) {
    this.pageId = pageId;
  }
  /**
   * notification id
   * @return nid
   **/
  @ApiModelProperty(example = "539721811729", value = "notification id")
  public String getNid() {
    return nid;
  }

  public void setNid(String nid) {
    this.nid = nid;
  }
  /**
   * notification type
   * @return ntype
   **/
  @ApiModelProperty(example = "HOT_ITEM", value = "notification type")
  public String getNtype() {
    return ntype;
  }

  public void setNtype(String ntype) {
    this.ntype = ntype;
  }
  /**
   * user name
   * @return userName
   **/
  @ApiModelProperty(example = "91334560c9v", value = "user name")
  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }
  /**
   * mc3 canonical message id (only for an mc3 message)
   * @return mc3id
   **/
  @ApiModelProperty(example = "1:763c4c33-b389-4016-b38e-83e29f82a1ba:2:70322535", value = "mc3 canonical message id (only for an mc3 message)")
  public String getMc3id() {
    return mc3id;
  }

  public void setMc3id(String mc3id) {
    this.mc3id = mc3id;
  }
  /**
   * notification action
   * @return pnact
   **/
  @ApiModelProperty(example = "1", value = "notification action")
  public String getPnact() {
    return pnact;
  }

  public void setPnact(String pnact) {
    this.pnact = pnact;
  }
  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EventPayload eventPayload = (EventPayload) o;
    return Objects.equals(this.pageId, eventPayload.pageId) &&
        Objects.equals(this.nid, eventPayload.nid) &&
        Objects.equals(this.ntype, eventPayload.ntype) &&
        Objects.equals(this.userName, eventPayload.userName) &&
        Objects.equals(this.mc3id, eventPayload.mc3id) &&
        Objects.equals(this.pnact, eventPayload.pnact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageId, nid, ntype, userName, mc3id, pnact);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EventPayload {\n");

    sb.append("    pageId: ").append(toIndentedString(pageId)).append("\n");
    sb.append("    nid: ").append(toIndentedString(nid)).append("\n");
    sb.append("    ntype: ").append(toIndentedString(ntype)).append("\n");
    sb.append("    userName: ").append(toIndentedString(userName)).append("\n");
    sb.append("    mc3id: ").append(toIndentedString(mc3id)).append("\n");
    sb.append("    pnact: ").append(toIndentedString(pnact)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}
