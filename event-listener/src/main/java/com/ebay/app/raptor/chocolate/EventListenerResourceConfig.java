package com.ebay.app.raptor.chocolate;

import com.ebay.raptor.dds.jaxrs.DDSTrackingFilter;
import com.ebay.tracking.filter.TrackingServiceFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Feature;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Resource config class
 *
 * @author xiangli4
 */
@Configuration
@EnableAsync
@ApplicationPath("/marketingtracking")
public class EventListenerResourceConfig extends Application {

  @Bean
  public Executor asyncExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    return executor;
  }

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
  @Qualifier("dds-tracking-filter")
  private DDSTrackingFilter ddsTrackingFilter;

  @Autowired
  @Qualifier("tracking-filter")
  private TrackingServiceFilter trackingFilter;

  @Autowired
  @Qualifier("user-prefs-filter")
  private ContainerRequestFilter userPrefsFilter;

  @Autowired
  @Qualifier("user-cultural-prefs-filter")
  private ContainerRequestFilter userCulturalPrefsFilter;

  @Autowired
  @Qualifier("geo-tracking-filter")
  private ContainerRequestFilter geoTrackingFilter;

  @Autowired
  @Qualifier("user-preferences-filter")
  private ContainerRequestFilter userPreferenceFilter;

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
    providers.add(ddsTrackingFilter);
    providers.add(trackingFilter);
    providers.add(userPrefsFilter);
    providers.add(userCulturalPrefsFilter);
    providers.add(geoTrackingFilter);
    providers.add(userPreferenceFilter);

    return providers;
  }
}
