package com.ebay.traffic.chocolate.mkttracksvc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.ebay.traffic.chocolate"})
public class MkttracksvcApplication {
  public static void main(String[] args) {
    SpringApplication.run(MkttracksvcApplication.class, args);
  }
}
