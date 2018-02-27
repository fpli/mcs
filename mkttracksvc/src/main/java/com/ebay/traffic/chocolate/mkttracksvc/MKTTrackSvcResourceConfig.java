package com.ebay.traffic.chocolate.mkttracksvc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Feature;
import java.util.LinkedHashSet;
import java.util.Set;

@Configuration
@ApplicationPath("/tracksvc/v1")
public class MKTTrackSvcResourceConfig extends Application {
  @Autowired
  @Qualifier("ets-feature")
  private Feature etsFeature;

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> providers = new LinkedHashSet<Class<?>>();
    providers.add(MKTTrackSvcResource.class);
    return providers;
  }

  @Override
  public Set<Object> getSingletons() {
    Set<Object> providers = new LinkedHashSet<Object>();
    providers.add(etsFeature);
    return providers;
  }

}
