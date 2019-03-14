package com.ebay.app.raptor.chocolate.eventlistener.error;

import com.ebay.standards.error.ErrorFactoryBuilder;
import com.ebay.standards.error.ErrorFactoryConfiguration;
import com.ebay.standards.error.v3.ErrorFactoryV3;
import com.ebay.standards.exception.ErrorResponseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.Response;

import static com.ebay.app.raptor.chocolate.eventlistener.constant.Errors.ERROR_CONTENT;
import static com.ebay.app.raptor.chocolate.eventlistener.constant.Errors.ERROR_DOMAIN;

/**
 * COS standard error maker
 *
 * @author xiangli4
 */
@Component
public class LocalizedErrorFactoryV3 {

  @Autowired
  ErrorFactoryBuilder builder;

  private ErrorFactoryV3 factory = null;

  @PostConstruct
  private void postConstruct() {
    ErrorFactoryConfiguration configuration = new ErrorFactoryConfiguration(
      ERROR_CONTENT,
      ERROR_DOMAIN).enableUseHttpCode();

    factory = builder.makeErrorFactoryV3(configuration);
  }

  /**
   * Create a COS exception with custom message.
   * @param errorName
   * @return
   */
  public ErrorResponseException makeException(String errorName) {
    Response response = factory.makeResponse(errorName);
    return factory.makeException(response);
  }

  public Response makeErrorResponse(String errorName) {
    return factory.makeResponse(errorName);
  }
}