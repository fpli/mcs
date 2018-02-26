package com.ebay.traffic.chocolate.mkttracksvc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;

@SpringBootApplication
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class MkttracksvcApplication {

  public static void main(String[] args) {
    SpringApplication.run(MkttracksvcApplication.class, args);
  }
}
