package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.traffic.chocolate.reportsvc.resource.ReportResource;
import com.ebay.traffic.chocolate.mkttracksvc.resource.RotationIdResource;
import com.ebay.traffic.chocolate.mkttracksvc.resource.SessionIdResource;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Application;
import java.util.LinkedHashSet;
import java.util.Set;


/**
 * Configuration File for HTTP Resources
 *
 * @author yimeng
 */
@Configuration
@ApplicationPath("/tracksvc/v1")
public class MKTTrackSvcResourceConfig extends ResourceConfig {

  @Autowired
  @Named("content-filter")
  ContainerRequestFilter contentFilter;

  @Inject
  @Named("permutation-filter")
  private ContainerRequestFilter permutationFilter;

  @Autowired(required = false)
  @Named("domain-request-filter")
  private ContainerRequestFilter domainRequestFilter;

  @PostConstruct
  public void postConstruct() {
    if (contentFilter != null) register(contentFilter);
    if (permutationFilter != null) register(permutationFilter);
    if (domainRequestFilter != null) register(domainRequestFilter);

    register(SessionIdResource.class);
    register(RotationIdResource.class);
    register(ReportResource.class);
  }
}
