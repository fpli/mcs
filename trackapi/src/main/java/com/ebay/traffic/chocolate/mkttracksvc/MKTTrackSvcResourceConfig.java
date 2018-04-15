package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.traffic.chocolate.mkttracksvc.resource.RotationIdResource;
import com.ebay.traffic.chocolate.mkttracksvc.resource.SessionIdResource;
import org.springframework.context.annotation.Configuration;

import javax.ws.rs.ApplicationPath;
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
public class MKTTrackSvcResourceConfig extends Application {

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> providers = new LinkedHashSet<Class<?>>();
    providers.add(SessionIdResource.class);
    providers.add(RotationIdResource.class);
    return providers;
  }
}
