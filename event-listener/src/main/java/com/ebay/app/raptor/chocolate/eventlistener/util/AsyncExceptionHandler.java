package com.ebay.app.raptor.chocolate.eventlistener.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * Handle async uncaught exceptions
 * @author xiangli4
 */
@Component
public class AsyncExceptionHandler implements AsyncUncaughtExceptionHandler {

  private final Logger logger = LoggerFactory.getLogger(AsyncExceptionHandler.class);

  @Override
  public void handleUncaughtException(Throwable ex, Method method, Object... params) {
    logger.warn("Unexpected asynchronous exception at : "
      + method.getDeclaringClass().getName() + "." + method.getName(), ex);
  }
}
