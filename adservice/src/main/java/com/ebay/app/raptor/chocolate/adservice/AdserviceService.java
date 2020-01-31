package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.jdbc.data.ThirdpartyWhitelistCache;
import com.ebay.app.raptor.chocolate.jdbc.repo.ThirdpartyWhitelistRepo;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Adservice entrance class
 *
 * @author - xiangli4
 */

@Configuration("AdserviceService")
@Singleton
public class AdserviceService {
  private static final Logger logger = LoggerFactory.getLogger(AdserviceService.class);

  private static final String ELASTICSEARCH_URL = "chocolate.adservice.elasticsearch.url";
  private static final String METRICS_INDEX_PREFIX = "chocolate.adservice.elasticsearch.index.prefix";

  @Autowired
  private ThirdpartyWhitelistRepo thirdpartyWhitelistRepo;

  @PostConstruct
  public void postInit() throws Exception {
    logger.info("Initializer called.");

    ApplicationOptions.init();
    ESMetrics.init(ApplicationOptions.getInstance().getByNameString(METRICS_INDEX_PREFIX), ApplicationOptions
      .getInstance().getByNameString(ELASTICSEARCH_URL));
    ApplicationOptions options = ApplicationOptions.getInstance();
    ThirdpartyWhitelistCache.init(thirdpartyWhitelistRepo);

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
