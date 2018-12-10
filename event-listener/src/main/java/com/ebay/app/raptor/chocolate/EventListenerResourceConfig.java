package com.ebay.app.raptor.chocolate;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Feature;

/**
 * Template resource config class
 *
 * @author xiangli4
 */
@Configuration
@ApplicationPath("/marketingtracking")
public class EventListenerResourceConfig extends ResourceConfig {

  @Inject
  @Named("jersey-operational-feature")
  private Feature jerseyOperationalFeature;

  @PostConstruct
  public void init() {
    register(jerseyOperationalFeature);
    register(EventListenerResource.class);
  }
}
