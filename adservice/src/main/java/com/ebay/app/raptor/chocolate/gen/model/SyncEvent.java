package com.ebay.app.raptor.chocolate.gen.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Objects;

@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2018-12-06T17:03:10.654+08:00[Asia/Shanghai]")
@JsonPropertyOrder({ "guid" })
@JsonIgnoreProperties(ignoreUnknown = true)
public class SyncEvent implements Serializable {

  private static final long serialVersionUID = 1L;



  @JsonProperty("guid")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String guid = null;

  /**
   * Get targetUrl
   * @return targetUrl
   **/
  @ApiModelProperty(example = "htttps://www.ebay.com/deals", value = "")
  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SyncEvent event = (SyncEvent) o;
    return Objects.equals(this.guid, event.guid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(guid);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SyncEvent {\n");

    sb.append("    guid: ").append(toIndentedString(guid)).append("\n");
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
