package com.ebay.traffic.chocolate.listener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;

@SpringBootApplication
@ComponentScan(value = "com.ebay.traffic.chocolate")
public class ListenerApplication extends AsyncConfigurerSupport {

  public static void main(String[] args) {
    SpringApplication.run(ListenerApplication.class, args);
  }

}
