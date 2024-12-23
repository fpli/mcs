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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.*;

/**
 * Tracking event payload
 */

@ApiModel(description = "Tracking event payload")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2020-07-02T09:57:48.715+08:00[Asia/Shanghai]")
@JsonPropertyOrder({ "pageId","tags","checkoutAPIClickTs" })
@JsonIgnoreProperties(ignoreUnknown = true)


public class EventPayload implements Serializable {

  private static final long serialVersionUID = 1L;



  @JsonProperty("pageId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long pageId = null;
  @JsonProperty("tags")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Map<String, String> tags = null;
  @JsonProperty("checkoutAPIClickTs")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String checkoutAPIClickTs = null;
  @JsonProperty("placeOfferAPIClickTs")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String placeOfferAPIClickTs = null;


  /**
   * page id, mandotary for notification
   * @return pageId
   **/
  @ApiModelProperty(example = "2054081", value = "page id, mandotary for notification")
  public Long getPageId() {
    return pageId;
  }

  public void setPageId(Long pageId) {
    this.pageId = pageId;
  }
  /**
   * Map&lt;String, String&gt; for soj tags
   * @return tags
   **/
  @ApiModelProperty(value = "Map<String, String> for soj tags")
  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  /**
   * checkoutAPIClickTs, mandotary for the click from checkoutAPI
   * @return checkoutAPIClickTs
   **/
  @ApiModelProperty(example = "1604475015939", value = "checkoutAPIClickTs, mandotary for click from checkoutAPI")
  public String getCheckoutAPIClickTs() {
    return checkoutAPIClickTs;
  }

  public void setCheckoutAPIClickTs(String checkoutAPIClickTs) {
    this.checkoutAPIClickTs = checkoutAPIClickTs;
  }

  /**
   * placeOfferAPIClickTs, mandotary for the click from placeOfferAPI
   * @return placeOfferAPIClickTs
   **/
  @ApiModelProperty(example = "1604475015939", value = "placeOfferAPIClickTs, mandotary for click from placeOfferAPI")
  public String getPlaceOfferAPIClickTs() {
    return placeOfferAPIClickTs;
  }

  public void setPlaceOfferAPIClickTs(String placeOfferAPIClickTs) {
    this.placeOfferAPIClickTs = placeOfferAPIClickTs;
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
        Objects.equals(this.tags, eventPayload.tags) &&
            Objects.equals(this.checkoutAPIClickTs, eventPayload.checkoutAPIClickTs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageId, tags, checkoutAPIClickTs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EventPayload {\n");

    sb.append("    pageId: ").append(toIndentedString(pageId)).append("\n");
    sb.append("    tags: ").append(toIndentedString(tags)).append("\n");
    sb.append("    checkoutAPIClickTs: ").append(toIndentedString(checkoutAPIClickTs)).append("\n");
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
