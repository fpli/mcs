package com.ebay.app.raptor.chocolate;

import com.ebay.tracking.filter.TrackingServiceFilter;
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
 * Resource config class
 *
 * @author xiangli4
 */
@Configuration
@ApplicationPath("/marketingtracking")
public class EventListenerResourceConfig extends ResourceConfig {

  @Inject
  @Named("jersey-operational-feature")
  private Feature jerseyOperationalFeature;

  @Inject
  @Named("content-filter")
  private ContainerRequestFilter contentFilter;

  @Inject
  @Named("permutation-filter")
  private ContainerRequestFilter permutationFilter;

  @Inject
  @Qualifier("cos-user-context-filter")
  private ContainerRequestFilter userCtxFilter;

  @Inject
  @Qualifier("core-auth-filter")
  private ContainerRequestFilter coreAuthFilter;

  @Inject
  @Qualifier("domain-request-filter")
  private ContainerRequestFilter domainRequestFilter;

  @Inject
  @Qualifier("dds-filter")
  private ContainerRequestFilter ddsFilter;

  @Inject
  @Qualifier("tracking-filter")
  private TrackingServiceFilter trackingFilter;

  @PostConstruct
  public void init() {
    register(jerseyOperationalFeature);
    register(contentFilter);
    register(permutationFilter);
    register(coreAuthFilter);
    register(userCtxFilter);
    register(domainRequestFilter);
    register(ddsFilter);
    register(trackingFilter);
    register(EventListenerResource.class);
  }
}
