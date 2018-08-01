package com.ebay.traffic.chocolate.init;

import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import org.eclipse.jetty.server.NetworkTrafficServerConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.net.URL;


@Configuration
@AutoConfigureAfter(ListenerAutoConfigure.class)
public class JettyServerConfiguration {

    private static final String LISTENER_OPTIONS = "chocolate-listener.xml";

    @Autowired
    Environment env;

    @Bean
    public JettyEmbeddedServletContainerFactory jettyEmbeddedServletContainerFactory() throws IOException {
        ListenerOptions.init(env.getProperty(LISTENER_OPTIONS, URL.class).openStream());

        ListenerOptions options = ListenerOptions.getInstance();

        JettyEmbeddedServletContainerFactory factory = new JettyEmbeddedServletContainerFactory(options.getInputHttpPort());

        factory.addServerCustomizers(new JettyServerCustomizer() {
            @Override
            public void customize(Server server) {
                final QueuedThreadPool threadPool = server.getBean(QueuedThreadPool.class);
                threadPool.setMaxThreads(options.getMaxThreads());
                final NetworkTrafficServerConnector connectorHttps = new NetworkTrafficServerConnector(server);
                connectorHttps.setPort(options.getInputHttpsPort());
                server.addConnector(connectorHttps);
            }
        });

        return factory;
    }
}
