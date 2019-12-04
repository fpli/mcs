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
 * ROIEvent
 */


@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-03T17:15:41.556+08:00[Asia/Shanghai]")
@JsonPropertyOrder({ "transType","uniqueTransactionId","itemId" })
@JsonIgnoreProperties(ignoreUnknown = true)


public class ROIEvent implements Serializable {

private static final long serialVersionUID = 1L;



    @JsonProperty("transType")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String transType = null;
    @JsonProperty("uniqueTransactionId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String uniqueTransactionId = null;
    @JsonProperty("itemId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String itemId = null;

/**
    * Get transType
* @return transType
    **/
    @ApiModelProperty(example = "BO-MobileApp", value = "")
public String getTransType() {
    return transType;
    }

public void setTransType(String transType) {
        this.transType = transType;
        }
/**
    * Get uniqueTransactionId
* @return uniqueTransactionId
    **/
    @ApiModelProperty(example = "1677235978009", value = "")
public String getUniqueTransactionId() {
    return uniqueTransactionId;
    }

public void setUniqueTransactionId(String uniqueTransactionId) {
        this.uniqueTransactionId = uniqueTransactionId;
        }
/**
    * Get itemId
* @return itemId
    **/
    @ApiModelProperty(example = "192658398245", value = "")
public String getItemId() {
    return itemId;
    }

public void setItemId(String itemId) {
        this.itemId = itemId;
        }
    @Override
    public boolean equals(Object o) {
    if (this == o) {
    return true;
    }
    if (o == null || getClass() != o.getClass()) {
    return false;
    }
        ROIEvent roIEvent = (ROIEvent) o;
        return Objects.equals(this.transType, roIEvent.transType) &&
        Objects.equals(this.uniqueTransactionId, roIEvent.uniqueTransactionId) &&
        Objects.equals(this.itemId, roIEvent.itemId);
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
