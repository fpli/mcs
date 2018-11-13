package com.ebay.app.raptor.chocolate.listener;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

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

  EventListenerService() {
    Properties log4jProps = new Properties();

    try (InputStream stategyRegistryConfigStream = new URL(
      RuntimeContext.getConfigRoot(), "log4j.properties")
      .openStream()) {
      log4jProps.load(stategyRegistryConfigStream);
      PropertyConfigurator.configure(log4jProps);

    } catch (IOException e) {
      BasicConfigurator.configure();
      logger.error("Fail to read " + RuntimeContext.getConfigRoot()
        + "log4j.properties, use default configure instead", e);
    }
  }

  @PostConstruct
  public void postInit() throws Exception {
    logger.info("Initializer called.");

    ApplicationOptions.init();
    ESMetrics.init(ApplicationOptions.getInstance().getByNameString(METRICS_INDEX_PREFIX), ApplicationOptions
      .getInstance().getByNameString(ELASTICSEARCH_URL));
    ApplicationOptions options = ApplicationOptions.getInstance();

    KafkaSink.initialize(options);
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
