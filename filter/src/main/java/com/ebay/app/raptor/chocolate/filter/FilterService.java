package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.filter.service.FilterContainer;
import com.ebay.app.raptor.chocolate.filter.service.FilterWorker;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@Singleton
public class FilterService {
  private static final Logger logger = LoggerFactory.getLogger(FilterService.class);

  private static final String FRONTIER_URL = "chocolate.filter.monitoring.url";
  private static final String FRONTIER_APPSVC = "chocolate.filter.monitoring.appSvc";
  private static final String ELASTICSEARCH_URL = "chocolate.filter.elasticsearch.url";
  private static final String TOPIC_THREAD_COUNT = "chocolate.filter.topic.threads";
  private static final String RULE_CONFIG_FILENAME = "filter_rule_config.json";
  private static final String METRICS_INDEX_PREFIX = "chocolate.filter.elasticsearch.index.prefix";
  private List<FilterWorker> workers = new ArrayList<>();

//  FilterService() {
//    Properties log4jProps = new Properties();
//
//    try (InputStream stategyRegistryConfigStream = new URL(
//        RuntimeContext.getConfigRoot(), "log4j.properties")
//        .openStream()) {
//      log4jProps.load(stategyRegistryConfigStream);
//      PropertyConfigurator.configure(log4jProps);
//
//    } catch (IOException e) {
//      BasicConfigurator.configure();
//      logger.error("Fail to read " + RuntimeContext.getConfigRoot()
//          + "log4j.properties, use default configure instead", e);
//    }
//  }

  @PostConstruct
  public void postInit() throws Exception {
    logger.info("Initializer called.");

    ApplicationOptions.init();
    LBSClient.init();
    ApplicationOptions options = ApplicationOptions.getInstance();
    //currently we need not use zookeeper watch to adding new campaign publisher pair into couchbase, so disable zookeeper here
   // FilterZookeeperClient.init(options);

    //Initial Rule Configuration Map
    ApplicationOptions.initFilterRuleConfig(RULE_CONFIG_FILENAME);

    KafkaSink.initialize(options);
    int topicThreadCount = options.getByNameInteger(TOPIC_THREAD_COUNT);

    FilterContainer filters = FilterContainer.createDefault(ApplicationOptions.filterRuleConfigMap);

    Map<KafkaCluster, Map<ChannelType, String>> kafkaConfigs = options.getInputKafkaConfigs();
    Map<ChannelType, String> sinkKafkaConfigs = options.getSinkKafkaConfigs();
    for (Map.Entry<KafkaCluster, Map<ChannelType, String>> kafkaConfig : kafkaConfigs.entrySet()) {
      KafkaCluster cluster = kafkaConfig.getKey();
      Properties properties = options.getInputKafkaProperties(cluster);

      Map<ChannelType, String> channelTopicMap = kafkaConfig.getValue();
      for (Map.Entry<ChannelType, String> channelTopic : channelTopicMap.entrySet()) {
        ChannelType channelType = channelTopic.getKey();
        String topic = channelTopic.getValue();
        for (int i = 0; i < topicThreadCount; i++) {
          FilterWorker worker = new FilterWorker(channelType, topic,
                  properties, sinkKafkaConfigs.get(channelType), filters);
          worker.start();
          workers.add(worker);
        }
      }
    }
  }

  @PreDestroy
  public void shutdown() {
    for (FilterWorker worker : workers) {
      worker.shutdown();
    }
    try {
      KafkaSink.close();
    } catch (IOException e) {
      logger.error("Shut down error", e);
    }
  }
}
