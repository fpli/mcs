package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.eventlistener.util.BehaviorMessageParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.ListenerMessageParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.RheosConsumerWrapper;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@Configuration("EventListenerService")
@Singleton
public class EventListenerService {
  private static final Logger logger = LoggerFactory.getLogger(EventListenerService.class);

  private static final String ELASTICSEARCH_URL = "chocolate.event-listener.elasticsearch.url";
  private static final String METRICS_INDEX_PREFIX = "chocolate.event-listener.elasticsearch.index.prefix";

  @PostConstruct
  public void postInit() throws Exception {
    logger.info("Initializer called.");

    ApplicationOptions.init();
    ESMetrics.init(ApplicationOptions.getInstance().getByNameString(METRICS_INDEX_PREFIX), ApplicationOptions
      .getInstance().getByNameString(ELASTICSEARCH_URL));
    ApplicationOptions options = ApplicationOptions.getInstance();

    KafkaSink.initialize(options, options);
    ListenerMessageParser.init();
    BehaviorMessageParser.init();

    RheosConsumerWrapper.init(ApplicationOptions.getInstance().getConsumeRheosKafkaProperties());
    RoverRheosTopicFilterTask.init(1000);
    RoverRheosTopicFilterTask.getInstance().start();
  }

  @PreDestroy
  public void shutdown() {
    try {
      KafkaSink.close();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
  }
}
