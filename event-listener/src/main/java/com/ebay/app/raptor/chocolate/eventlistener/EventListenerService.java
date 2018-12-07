package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.eventlistener.util.ListenerMessageParser;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Event listener service class
 *
 * @author - xiangli4
 */

@Configuration
@Singleton
public class EventListenerService {
  private static final Logger logger = Logger.getLogger(EventListenerService.class);

  private static final String ELASTICSEARCH_URL = "chocolate.event-listener.elasticsearch.url";
  private static final String METRICS_INDEX_PREFIX = "chocolate.event-listener.elasticsearch.index.prefix";

  @PostConstruct
  public void postInit() throws Exception {
    logger.info("Initializer called.");

    ApplicationOptions.init();
    ESMetrics.init(ApplicationOptions.getInstance().getByNameString(METRICS_INDEX_PREFIX), ApplicationOptions
      .getInstance().getByNameString(ELASTICSEARCH_URL));
    ApplicationOptions options = ApplicationOptions.getInstance();

    KafkaSink.initialize(options);
    ListenerMessageParser.init();
  }

  @PreDestroy
  public void shutdown() {
    try {
      KafkaSink.close();
    } catch (IOException e) {
      logger.error(e);
    }
  }
}
