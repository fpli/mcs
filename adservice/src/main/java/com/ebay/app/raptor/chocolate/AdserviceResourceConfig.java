package com.ebay.app.raptor.chocolate;

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
public class AdserviceResourceConfig extends Application {

  @Autowired
  @Qualifier("ets-feature")
  private Feature etsFeature;

  @Autowired
  @Qualifier("cookie-filter")
  private ContainerRequestFilter cookieFilter;

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> providers = new LinkedHashSet<Class<?>>();
    providers.add(AdserviceResource.class);
    return providers;
  }

  @Override
  public Set<Object> getSingletons() {
    Set<Object> providers = new LinkedHashSet<Object>();
    providers.add(cookieFilter);
    providers.add(etsFeature);
    return providers;
  }
}
