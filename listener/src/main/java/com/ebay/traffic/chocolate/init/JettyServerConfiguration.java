package com.ebay.traffic.chocolate.init;

import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.app.raptor.chocolate.common.Hostname;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.traffic.chocolate.jdbc.repo.DriverIdServiceImpl;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.NetworkTrafficServerConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

@Configuration
public class JettyServerConfiguration {
  private static final Logger logger = Logger.getLogger(JettyServerConfiguration.class);

  private static final String LISTENER_OPTIONS = "chocolate-listener.xml";
  private static final int DRIVERID_RETRIES = 10;

  @Autowired
  Environment env;

  @Autowired
  private DriverIdServiceImpl driverIdService;

  @SuppressWarnings("unchecked")
  @Bean
  public ConfigurableServletWebServerFactory webServerFactory() throws IOException
  {
    URL property = env.getProperty(LISTENER_OPTIONS, URL.class);
    if (property == null) {
      throw new IllegalArgumentException("listener options is null");
    } else {
      InputStream inputStream = property.openStream();
      ListenerOptions.init(inputStream);

      ListenerOptions options = ListenerOptions.getInstance();

      int driverId = driverIdService.getDriverId(Hostname.HOSTNAME, Hostname.getIp(), Long.valueOf(SnapshotId.MAX_DRIVER_ID).intValue(), DRIVERID_RETRIES);
      if (driverId != -1) {
        ESMetrics.getInstance().meter("DriverIdFromDB", 1, Field.of("ip", Hostname.IP), Field.of("driver_id", driverId));
        options.setDriverId(driverId);
      } else {
        driverId = ApplicationOptionsParser.getDriverIdFromIp();
        logger.error("get driver id from db failed, try to generate random driver id");
        ESMetrics.getInstance().meter("RandomDriverId", 1, Field.of("ip", Hostname.IP), Field.of("driver_id", driverId));
        options.setDriverId(driverId);
      }

      ListenerInitializer.init(ListenerOptions.getInstance());

      JettyServletWebServerFactory factory = new JettyServletWebServerFactory(options.getInputHttpPort());
      factory.addServerCustomizers(new JettyServerCustomizer() {
        @Override
        public void customize(Server server) {
          final QueuedThreadPool threadPool = server.getBean(QueuedThreadPool.class);
          threadPool.setMaxThreads(options.getMaxThreads());
          final NetworkTrafficServerConnector connectorHttps = new NetworkTrafficServerConnector(server);
          connectorHttps.setPort(options.getInputHttpsPort());
          server.addConnector(connectorHttps);
          final NetworkTrafficServerConnector connectorValidateInternals = new NetworkTrafficServerConnector(server);
          connectorValidateInternals.setPort(options.getValidateInternalsPort());
          server.addConnector(connectorValidateInternals);
        }
      });
      return factory;
    }
  }

  @PreDestroy
  public void destory() {
    ListenerInitializer.terminate();
  }
}
