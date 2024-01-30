package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.common.DAPRvrId;
import com.ebay.app.raptor.chocolate.common.Hostname;
import com.ebay.app.raptor.chocolate.jdbc.data.ThirdpartyWhitelistCache;
import com.ebay.app.raptor.chocolate.jdbc.repo.DriverIdServiceImpl;
import com.ebay.app.raptor.chocolate.jdbc.repo.ThirdpartyWhitelistRepo;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
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

  private static final String DRIVERID_RETRIES = "chocolate.adservice.driverid.retries";

  @Autowired
  private ThirdpartyWhitelistRepo thirdpartyWhitelistRepo;

  @Autowired
  private DriverIdServiceImpl driverIdService;

  @PostConstruct
  public void postInit() throws Exception {
    logger.info("Initializer called.");

    ApplicationOptions.init();
    ApplicationOptions options = ApplicationOptions.getInstance();
    options.setDriverId(driverIdService.getDriverId(Hostname.HOSTNAME, Hostname.getIp(), DAPRvrId.getMaxDriverId(), ApplicationOptions.getInstance().getByNameInteger(DRIVERID_RETRIES)));
    options.loadAttestationFile();
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
