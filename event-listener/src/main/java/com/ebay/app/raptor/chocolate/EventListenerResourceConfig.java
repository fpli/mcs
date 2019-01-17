package com.ebay.app.raptor.chocolate;

import com.ebay.tracking.filter.TrackingServiceFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Feature;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Resource config class
 *
 * @author xiangli4
 */
@Configuration
@ApplicationPath("/marketingtracking")
public class EventListenerResourceConfig extends Application {

  @Autowired
  @Qualifier("jersey-operational-feature")
  private Feature jerseyOperationalFeature;

  @Autowired
  @Qualifier("content-filter")
  private ContainerRequestFilter contentFilter;

  @Autowired
  @Qualifier("permutation-filter")
  private ContainerRequestFilter permutationFilter;

  @Autowired
  @Qualifier("cos-user-context-filter")
  private ContainerRequestFilter userCtxFilter;

  @Autowired
  @Qualifier("core-auth-filter")
  private ContainerRequestFilter coreAuthFilter;

  @Autowired
  @Qualifier("domain-request-filter")
  private ContainerRequestFilter domainRequestFilter;

  @Autowired
  @Qualifier("dds-filter")
  private ContainerRequestFilter ddsFilter;

  @Autowired
  @Qualifier("tracking-filter")
  private TrackingServiceFilter trackingFilter;

  // comments geo info feature first, as this requires raptor-io 0.8.x
//  @Inject
//  @Qualifier("user-prefs-filter")
//  private ContainerRequestFilter userPrefsFilter;
//
//  @Inject
//  @Qualifier("user-cultural-prefs-filter")
//  private ContainerRequestFilter userCulturalPrefsFilter;
//
//  @Inject
//  @Qualifier("geo-tracking-filter")
//  private ContainerRequestFilter geoTrackingFilter;
//
//  @Inject
//  @Qualifier("user-preferences-filter")
//  private ContainerRequestFilter userPreferenceFilter;

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> providers = new LinkedHashSet<Class<?>>();
    providers.add(EventListenerResource.class);
    return providers;
  }

  @Override
  public Set<Object> getSingletons() {
    Set<Object> providers = new LinkedHashSet<Object>();
    providers.add(jerseyOperationalFeature);
    providers.add(contentFilter);
    providers.add(permutationFilter);
    providers.add(coreAuthFilter);
    providers.add(userCtxFilter);
    providers.add(domainRequestFilter);
    providers.add(ddsFilter);
    providers.add(trackingFilter);
//    providers.add(userPrefsFilter);
//    providers.add(userCulturalPrefsFilter);
//    providers.add(geoTrackingFilter);
//    providers.add(userPreferenceFilter);

    return providers;
  }

//  @PostConstruct
//  public void init() {
//    register(jerseyOperationalFeature);
//    register(contentFilter);
//    register(permutationFilter);
//    register(coreAuthFilter);
//    register(userCtxFilter);
//    register(domainRequestFilter);
//    register(ddsFilter);
//    register(trackingFilter);
////    register(userPrefsFilter);
////    register(userCulturalPrefsFilter);
////    register(geoTrackingFilter);
////    register(userPreferenceFilter);
//    register(EventListenerResource.class);
//  }
}
