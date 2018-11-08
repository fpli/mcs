package com.ebay.app.raptor.chocolate.seed;

import com.ebay.app.raptor.chocolate.common.Environment;
import com.ebay.app.raptor.chocolate.seed.service.SeedWorker;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.KafkaSink2;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@Singleton
public class SeedService {
  private static final Logger logger = Logger.getLogger(SeedService.class);

  private static final String ELASTICSEARCH_URL = "chocolate.seed.elasticsearch.url";
  private static final String TOPIC_THREAD_COUNT = "chocolate.seed.topic.threads";
  private static final String METRICS_INDEX_PREFIX = "chocolate.seed.elasticsearch.index.prefix";
  private final String LOG4J_FILE = "log4j.properties";
  private List<SeedWorker> workers = new ArrayList<>();

  SeedService() {
    Properties log4jProps = new Properties();
    InputStream inputStream;
    try {
      if (RuntimeContext.getConfigRoot() == null) {
        inputStream = SeedService.class.getClassLoader().getResourceAsStream(LOG4J_FILE);
      } else {
        inputStream = new URL(
            RuntimeContext.getConfigRoot(), "log4j.properties")
            .openStream();
      }
      log4jProps.load(inputStream);
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
    ESMetrics.init(ApplicationOptions.getInstance().getByNameString(METRICS_INDEX_PREFIX), ApplicationOptions.getInstance().getByNameString(ELASTICSEARCH_URL));
    ApplicationOptions options = ApplicationOptions.getInstance();

    KafkaSink2.initialize(options);
    int topicThreadCount = options.getByNameInteger(TOPIC_THREAD_COUNT);
    String pjEndpoint = options.getPJEndPoint();

    Map<KafkaCluster, Map<String, String>> kafkaConfigs = options.getInputKafkaConfigs();
    Map<String, String> sinkKafkaConfigs = options.getSinkKafkaConfigs();
    for (Map.Entry<KafkaCluster, Map<String, String>> kafkaConfig : kafkaConfigs.entrySet()) {
      KafkaCluster cluster = kafkaConfig.getKey();
      Properties properties = options.getInputKafkaProperties(cluster);

      Map<String, String> channelTopicMap = kafkaConfig.getValue();
      for (Map.Entry<String, String> channelTopic : channelTopicMap.entrySet()) {
        String channelType = channelTopic.getKey();
        String topic = channelTopic.getValue();
        for (int i = 0; i < topicThreadCount; i++) {
          SeedWorker worker = new SeedWorker(channelType, topic, properties, sinkKafkaConfigs.get(channelType), pjEndpoint);
          worker.start();
          workers.add(worker);
        }
      }
    }
  }

  @PreDestroy
  public void shutdown() {
    for (SeedWorker worker : workers) {
      worker.shutdown();
    }
    try {
      KafkaSink.close();
    } catch (IOException e) {
      logger.error(e);
    }
  }
}
