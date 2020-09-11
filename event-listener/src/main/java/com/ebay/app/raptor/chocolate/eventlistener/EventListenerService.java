package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.app.raptor.chocolate.common.DAPRvrId;
import com.ebay.app.raptor.chocolate.common.Hostname;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.eventlistener.util.BehaviorMessageParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.ListenerMessageParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.RheosConsumerWrapper;
import com.ebay.app.raptor.chocolate.jdbc.repo.DriverIdServiceImpl;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
  private static final String DRIVERID_RETRIES = "chocolate.event-listener.driverid.retries";

  @Autowired
  private DriverIdServiceImpl driverIdService;

  @SuppressWarnings("unchecked")
  @PostConstruct
  public void postInit() throws Exception {
    logger.info("Initializer called.");

    ApplicationOptions.init();
    ESMetrics.init(ApplicationOptions.getInstance().getByNameString(METRICS_INDEX_PREFIX), ApplicationOptions
      .getInstance().getByNameString(ELASTICSEARCH_URL));
    ApplicationOptions options = ApplicationOptions.getInstance();
    int driverId = driverIdService.getDriverId(Hostname.HOSTNAME, Hostname.getIp(), Long.valueOf(SnapshotId.MAX_DRIVER_ID).intValue(), ApplicationOptions.getInstance().getByNameInteger(DRIVERID_RETRIES));
    if (driverId != -1) {
      ESMetrics.getInstance().meter("DriverIdFromDB", 1, Field.of("ip", Hostname.IP), Field.of("driver_id", driverId));
      options.setDriverId(driverId);
    } else {
      driverId = ApplicationOptionsParser.getDriverIdFromIp();
      logger.error("get driver id from db failed, try to generate random driver id");
      ESMetrics.getInstance().meter("RandomDriverId", 1, Field.of("ip", Hostname.IP), Field.of("driver_id", driverId));
      options.setDriverId(driverId);
    }

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
