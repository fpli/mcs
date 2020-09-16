package com.ebay.traffic.chocolate.listener;

import com.ebay.traffic.chocolate.listener.configuration.DatabaseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;

@SpringBootApplication
@ComponentScan(value = "com.ebay.traffic.chocolate")
@EntityScan("com.ebay.traffic.chocolate")
@EnableJpaRepositories("com.ebay.traffic.chocolate")
@EnableConfigurationProperties({
        DatabaseProperties.class
})
public class ListenerApplication extends AsyncConfigurerSupport {

  public static void main(String[] args) {
    SpringApplication.run(ListenerApplication.class, args);
  }

}
