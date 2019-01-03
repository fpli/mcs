package com.ebay.app.raptor.chocolate.eventlistener.constant;

/**
 * ErrorKey is registered in marketingTrackingDomain.4cb
 * Change on this class should have same error key registered in 4cb
 * @author xiangli4
 */
public enum ErrorType {
  NO_USER_AGENT(4000, "NoUserAgent", Errors.ERROR_NO_USER_AGENT),
  NO_ENDUSERCTX(4001, "NoEndUserCtx", Errors.ERROR_NO_ENDUSERCTX),
  NO_TRACKING(4002, "NoTracking", Errors.ERROR_NO_TRACKING),
  NO_REFERER(4003, "NoReferer", Errors.ERROR_NO_REFERER),
  ILLEGAL_URL(4004, "IllegalUrl", Errors.ERROR_ILLEGAL_URL),
  NO_QUERY_PARAMETER(4005, "NoQueryParameter", Errors.ERROR_NO_QUERY_PARAMETER),
  NO_MKEVT(4006, "NoMkevt", Errors.ERROR_NO_MKEVT),
  INVALID_MKEVT(4007, "InvalidMkevt", Errors.ERROR_INVALID_MKEVT),
  INVALID_ENDUSERCTX(4008, "InvalidEndUserCtx", Errors.ERROR_INVALID_ENDUSERCTX),
  INTERNAL_SERVICE_ERROR(5000, "InternalServiceError", Errors.ERROR_INTERNAL_SERVICE);

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
