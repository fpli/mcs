package com.ebay.traffic.chocolate.init;

import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Configuration
public class ListenerAutoConfigure {
   private static final Logger logger = Logger.getLogger(ListenerAutoConfigure.class);

   private static final int EXIT_NORMAL = 0;

   private static final int EXIT_ERROR = 1;

   private static final int EXIT_COULD_NOT_SHUTDOWN = 2;

   private static int returnCode = EXIT_NORMAL;

   @PreDestroy
   public void destroy() {
      ListenerInitializer.terminate();
   }

   @PostConstruct
   public void init() {
      logger.info("Starting Listener");
      ListenerInitializer.init(ListenerOptions.getInstance());
   }
}
