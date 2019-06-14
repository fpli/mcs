package com.ebay.app.raptor.chocolate.eventlistener.error;

import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.standards.error.ErrorFactoryBuilder;
import com.ebay.standards.error.ErrorFactoryConfiguration;
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

  @PostConstruct
  private void postConstruct() {
    ErrorFactoryConfiguration configuration = new ErrorFactoryConfiguration(
      ERROR_CONTENT,
      ERROR_DOMAIN).enableUseHttpCode();
  }

  /**
   * We don't return bad request even the incoming is invalid.
   * As bot callings are all with null referer, we will get lots of ERROR in CAL in domain pages.
   * @param errorName
   * @return
   */
  public Response makeWarnResponse(String errorName) {
    return Response.status(Response.Status.OK).entity((Errors.errorMap.get(errorName))).build();
  }
}