package com.ebay.traffic.chocolate.reportsvc.constant;

public enum ErrorType {
  INTERNAL_SERVICE_ERROR(10001, "InternalError", Errors.INTERNAL_ERROR_MSG),
  BAD_PARTNER_INFO(10002, "BadPartnerInfo", Errors.BAD_PARTNER_MSG),
  BAD_CAMPAIGN_INFO(10003, "BadCampaignInfo", Errors.BAD_CAMPAIGN_MSG),
  BAD_START_END_DATE(10004, "BadStartEndDate", Errors.BAD_DATES_MSG),
  EXPIRED_REQUEST(10005, "ExpiredRequest", Errors.EXPIRED_MSG),
  UNAUTHORIZED_REQUEST(10006, "UnauthorizedRequest", Errors.UNAUTHORIZED_MSG);

  private int errorCode;
  private String errorKey;
  private String errorMessage;

  ErrorType(int errorCode, String errorKey, String errorMessage) {
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

  public static ErrorType getErrorTypeByErrorCode(String errorCode) {
    for (ErrorType errorType : ErrorType.values()) {
      if (errorType.errorKey.equalsIgnoreCase(errorCode)) {
        return errorType;
      }
    }
    return ErrorType.INTERNAL_SERVICE_ERROR;
  }

}
