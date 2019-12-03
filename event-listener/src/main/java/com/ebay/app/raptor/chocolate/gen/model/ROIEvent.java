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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;

/**
 * ROIEvent
 */


@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2018-12-06T17:03:10.654+08:00[Asia/Shanghai]")
@JsonPropertyOrder({ "transType","uniqueTransactionId","itemId" })
@JsonIgnoreProperties(ignoreUnknown = true)


public class ROIEvent implements Serializable {

  private static final long serialVersionUID = 1L;
  @JsonProperty("transType")
  //@JsonInclude(JsonInclude.Include.NON_NULL)
  private String transType = "";
  @JsonProperty("uniqueTransactionId")
 // @JsonInclude(JsonInclude.Include.NON_NULL)
  private String uniqueTransactionId = "";
  @JsonProperty("itemId")
  //@JsonInclude(JsonInclude.Include.NON_NULL)
  private String itemId = "";

  /**
   * Get transType
   * @return transType
   **/
 // @ApiModelProperty(example = "htttps://www.ebay.com/deals", value = "")
  public String getTransType() {
    return transType;
  }

  public void setTransType(String transType) {
    this.transType = transType;
  }
  /**
   * Get UniqueTransactionId
   * @return UniqueTransactionId
   **/
 // @ApiModelProperty(example = "htttps://www.google.com", value = "")
  public String getUniqueTransactionId() {
    return uniqueTransactionId;
  }

  public void setUniqueTransactionId(String uniqueTransactionId) {
    this.uniqueTransactionId = uniqueTransactionId;
  }

  public String getItemId() {
    return itemId;
  }

  public void setItemId(String itemId) {this.itemId = itemId;}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ROIEvent event = (ROIEvent) o;
    return Objects.equals(this.transType, event.transType) &&
      Objects.equals(this.uniqueTransactionId, event.uniqueTransactionId) &&
        Objects.equals(this.itemId, event.itemId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transType, uniqueTransactionId, itemId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ROIEvent {\n");
    sb.append("    transType: ").append(toIndentedString(transType)).append("\n");
    sb.append("    uniqueTransactionId: ").append(toIndentedString(uniqueTransactionId)).append("\n");
    sb.append("    itemId: ").append(toIndentedString(itemId)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}
