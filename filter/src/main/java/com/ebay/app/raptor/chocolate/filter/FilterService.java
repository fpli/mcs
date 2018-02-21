package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.filter.service.FilterContainer;
import com.ebay.app.raptor.chocolate.filter.service.FilterWorker;
import com.ebay.app.raptor.chocolate.filter.util.FilterZookeeperClient;
import com.ebay.kernel.context.RuntimeContext;
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

@Configuration
@Singleton
public class FilterService {
  // Logger
  private static final Logger logger = Logger.getLogger(FilterService.class);

  private static final String FRONTIER_URL = "chocolate.filter.monitoring.url";
  private static final String FRONTIER_APPSVC = "chocolate.filter.monitoring.appSvc";
  private static final String THREAD_COUNT = "chocolate.filter.threads";
  private static final String RULE_CONFIG_FILENAME = "filter_rule_config.json";
  private FilterWorker[] filterThreads;

  FilterService() {
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

    ApplicationOptions.init("filter.properties", "filter-kafka.properties");
    MetricsClient.init(ApplicationOptions.getInstance().getByNameString(FRONTIER_URL), ApplicationOptions.getInstance().getByNameString(FRONTIER_APPSVC));
    FilterZookeeperClient.init(ApplicationOptions.getInstance());

    //Initial Rule Configuration Map
    ApplicationOptions.initFilterRuleConfig( RULE_CONFIG_FILENAME);
    
    int threadCount = ApplicationOptions.getInstance().getByNameInteger(THREAD_COUNT);
    this.filterThreads = new FilterWorker[threadCount];
    for (int i = 0; i < threadCount; ++i) {
      KafkaWrapper kafka = new KafkaWrapper();
      FilterContainer filterSet = FilterContainer.createDefault(ApplicationOptions.filterRuleConfigMap);
      this.filterThreads[i] = new FilterWorker(filterSet, kafka, MetricsClient.getInstance());
      this.filterThreads[i].start();
    }
  }

  @PreDestroy
  public void shutdown() {
    for (FilterWorker thread : this.filterThreads) {
      thread.shutdown();
    }
  }
}
