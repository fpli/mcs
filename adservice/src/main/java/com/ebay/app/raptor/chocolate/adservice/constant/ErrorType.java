package com.ebay.app.raptor.chocolate.adservice.constant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author xiangli4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class ErrorType {

  private int errorCode;
  private String errorKey;
  private String errorMessage;

  public ErrorType() {}

  public ErrorType(int errorCode, String errorKey, String errorMessage) {
    this.errorCode = errorCode;
    this.errorKey = errorKey;
    this.errorMessage = errorMessage;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getErrorKey() {
    return errorKey;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

}
