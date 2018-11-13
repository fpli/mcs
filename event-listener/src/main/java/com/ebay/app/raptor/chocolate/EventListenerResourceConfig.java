package com.ebay.app.raptor.chocolate;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.context.annotation.Configuration;

import javax.ws.rs.ApplicationPath;

@Configuration
@ApplicationPath("/samplesvc")
public class EventListenerResourceConfig extends ResourceConfig {

  public EventListenerResourceConfig() {
    register(EventListenerResource.class);
  }
}
