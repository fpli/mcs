package com.ebay.app.raptor.chocolate;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ebay.raptor.tomcat.autoconfigure.CustomizedTomcatThreadExecutor;
import com.ebay.raptor.tomcat.autoconfigure.RaptorServletContainerCustomizerBeanConfiguration;

@Configuration
@EnableConfigurationProperties(ServerProperties.class)
@AutoConfigureAfter(RaptorServletContainerCustomizerBeanConfiguration.class)
public class TomcatWebServerBeanConfig {
	
    @Bean
    //@ConditionalOnBean(RaptorTomcatCustomizer.class)
    public TomcatWebServerCustomizer tomcatWebServerCustomizer(ServerProperties serverProperties,
        CustomizedTomcatThreadExecutor executor) {
      return new TomcatWebServerCustomizer();
    }
}
