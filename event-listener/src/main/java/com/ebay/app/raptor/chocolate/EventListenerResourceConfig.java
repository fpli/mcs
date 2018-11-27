package com.ebay.app.raptor.chocolate;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.container.ContainerRequestFilter;

/**
 * Template resource config class
 *
 * @author xiangli4
 */
@Configuration
@ApplicationPath("/marketingtracking")
public class EventListenerResourceConfig extends ResourceConfig {

  @Inject
  @Qualifier("core-auth-filter")
  ContainerRequestFilter coreAuthFilter;


  @PostConstruct
  public void init() {
    register(coreAuthFilter);
  }

  public EventListenerResourceConfig() {
    register(EventListenerResource.class);
  }
}
