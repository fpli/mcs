package com.ebay.traffic.chocolate.listener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;

@SpringBootApplication
public class ListenerApplication extends AsyncConfigurerSupport {

   public static void main(String[] args) {
      SpringApplication.run(ListenerApplication.class, args);
   }

}
