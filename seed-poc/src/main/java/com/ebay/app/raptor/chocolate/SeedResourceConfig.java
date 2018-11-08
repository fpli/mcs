package com.ebay.app.raptor.chocolate;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.context.annotation.Configuration;

import javax.ws.rs.ApplicationPath;

@Configuration
@ApplicationPath("/samplesvc")
public class SeedResourceConfig extends ResourceConfig {

  public SeedResourceConfig() {
    register(SeedResource.class);
  }
}
