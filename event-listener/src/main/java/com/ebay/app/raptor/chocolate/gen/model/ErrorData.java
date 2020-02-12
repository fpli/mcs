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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.*;

/**
 * Detail of each error modeled based on the cos error and exceptions spec:  https://github.corp.ebay.com/commerceos/cos-error-and-exceptions/blob/master/error-and-exception-handling.md 
 */

@ApiModel(description = "Detail of each error modeled based on the cos error and exceptions spec:  https://github.corp.ebay.com/commerceos/cos-error-and-exceptions/blob/master/error-and-exception-handling.md ")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-03T17:15:41.556+08:00[Asia/Shanghai]")
@JsonPropertyOrder({ "errorId","domain","subdomain","category","message","longMessage","inputRefIds","outputRefIds","parameters" })
@JsonIgnoreProperties(ignoreUnknown = true)


public class ErrorData implements Serializable {

private static final long serialVersionUID = 1L;



    @JsonProperty("errorId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private BigDecimal errorId = null;
            /**
            * Gets or Sets domain
            */
            public enum DomainEnum {
            
MARKETING("Marketing");            
              private String value;
            
              DomainEnum(String value) {
                this.value = value;
              }
            
              @JsonValue
                public String getValue() {
                return value;
              }
            
              @Override
              public String toString() {
                return String.valueOf(value);
              }
            
              @JsonCreator
              public static DomainEnum fromValue(String text) {
                for (DomainEnum b : DomainEnum.values()) {
                  if (String.valueOf(b.value).equals(text)) {
                    return b;
                  }
                }
                return null;
              }
            
            }
    @JsonProperty("domain")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private DomainEnum domain = null;
            /**
            * Gets or Sets subdomain
            */
            public enum SubdomainEnum {
            
TRACKING("Tracking");            
              private String value;
            
              SubdomainEnum(String value) {
                this.value = value;
              }
            
              @JsonValue
                public String getValue() {
                return value;
              }
            
              @Override
              public String toString() {
                return String.valueOf(value);
              }
            
              @JsonCreator
              public static SubdomainEnum fromValue(String text) {
                for (SubdomainEnum b : SubdomainEnum.values()) {
                  if (String.valueOf(b.value).equals(text)) {
                    return b;
                  }
                }
                return null;
              }
            
            }
    @JsonProperty("subdomain")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private SubdomainEnum subdomain = null;
            /**
            * Gets or Sets category
            */
            public enum CategoryEnum {
            
REQUEST("REQUEST"),
                      APPLICATION("APPLICATION");            
              private String value;
            
              CategoryEnum(String value) {
                this.value = value;
              }
            
              @JsonValue
                public String getValue() {
                return value;
              }
            
              @Override
              public String toString() {
                return String.valueOf(value);
              }
            
              @JsonCreator
              public static CategoryEnum fromValue(String text) {
                for (CategoryEnum b : CategoryEnum.values()) {
                  if (String.valueOf(b.value).equals(text)) {
                    return b;
                  }
                }
                return null;
              }
            
            }
    @JsonProperty("category")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private CategoryEnum category = null;
    @JsonProperty("message")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String message = null;
    @JsonProperty("longMessage")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String longMessage = null;
    @JsonProperty("inputRefIds")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<String> inputRefIds = null;
    @JsonProperty("outputRefIds")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<String> outputRefIds = null;
    @JsonProperty("parameters")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<String> parameters = null;

/**
    * Value indicates type of error
* @return errorId
    **/
    @ApiModelProperty(value = "Value indicates type of error")
public BigDecimal getErrorId() {
    return errorId;
    }

public void setErrorId(BigDecimal errorId) {
        this.errorId = errorId;
        }
/**
    * Get domain
* @return domain
    **/
    @ApiModelProperty(value = "")
public DomainEnum getDomain() {
    return domain;
    }

public void setDomain(DomainEnum domain) {
        this.domain = domain;
        }
/**
    * Get subdomain
* @return subdomain
    **/
    @ApiModelProperty(value = "")
public SubdomainEnum getSubdomain() {
    return subdomain;
    }

public void setSubdomain(SubdomainEnum subdomain) {
        this.subdomain = subdomain;
        }
/**
    * Get category
* @return category
    **/
    @ApiModelProperty(example = "{\"REQUEST\":\"Incorrect request e.g. auth, headers etc (Error Code 4xxx)\",\"APPLICATION\":\"Service related runtime exception (Error Code 5xxx)\"}", value = "")
public CategoryEnum getCategory() {
    return category;
    }

public void setCategory(CategoryEnum category) {
        this.category = category;
        }
/**
    * Get message
* @return message
    **/
    @ApiModelProperty(value = "")
public String getMessage() {
    return message;
    }

public void setMessage(String message) {
        this.message = message;
        }
/**
    * Get longMessage
* @return longMessage
    **/
    @ApiModelProperty(value = "")
public String getLongMessage() {
    return longMessage;
    }

public void setLongMessage(String longMessage) {
        this.longMessage = longMessage;
        }
/**
    * Get inputRefIds
* @return inputRefIds
    **/
    @ApiModelProperty(value = "")
public List<String> getInputRefIds() {
    return inputRefIds;
    }

public void setInputRefIds(List<String> inputRefIds) {
        this.inputRefIds = inputRefIds;
        }
/**
    * Get outputRefIds
* @return outputRefIds
    **/
    @ApiModelProperty(value = "")
public List<String> getOutputRefIds() {
    return outputRefIds;
    }

public void setOutputRefIds(List<String> outputRefIds) {
        this.outputRefIds = outputRefIds;
        }
/**
    * Get parameters
* @return parameters
    **/
    @ApiModelProperty(value = "")
public List<String> getParameters() {
    return parameters;
    }

public void setParameters(List<String> parameters) {
        this.parameters = parameters;
        }
    @Override
    public boolean equals(Object o) {
    if (this == o) {
    return true;
    }
    if (o == null || getClass() != o.getClass()) {
    return false;
    }
        ErrorData errorData = (ErrorData) o;
        return Objects.equals(this.errorId, errorData.errorId) &&
        Objects.equals(this.domain, errorData.domain) &&
        Objects.equals(this.subdomain, errorData.subdomain) &&
        Objects.equals(this.category, errorData.category) &&
        Objects.equals(this.message, errorData.message) &&
        Objects.equals(this.longMessage, errorData.longMessage) &&
        Objects.equals(this.inputRefIds, errorData.inputRefIds) &&
        Objects.equals(this.outputRefIds, errorData.outputRefIds) &&
        Objects.equals(this.parameters, errorData.parameters);
    }

    @Override
    public int hashCode() {
    return Objects.hash(errorId, domain, subdomain, category, message, longMessage, inputRefIds, outputRefIds, parameters);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("class ErrorData {\n");
      
      sb.append("    errorId: ").append(toIndentedString(errorId)).append("\n");
      sb.append("    domain: ").append(toIndentedString(domain)).append("\n");
      sb.append("    subdomain: ").append(toIndentedString(subdomain)).append("\n");
      sb.append("    category: ").append(toIndentedString(category)).append("\n");
      sb.append("    message: ").append(toIndentedString(message)).append("\n");
      sb.append("    longMessage: ").append(toIndentedString(longMessage)).append("\n");
      sb.append("    inputRefIds: ").append(toIndentedString(inputRefIds)).append("\n");
      sb.append("    outputRefIds: ").append(toIndentedString(outputRefIds)).append("\n");
      sb.append("    parameters: ").append(toIndentedString(parameters)).append("\n");
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
