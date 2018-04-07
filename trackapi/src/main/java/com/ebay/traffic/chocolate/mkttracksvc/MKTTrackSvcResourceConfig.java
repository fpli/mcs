package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.traffic.chocolate.mkttracksvc.resource.RotationIdSvc;
import com.ebay.traffic.chocolate.mkttracksvc.resource.SessionIdSvc;
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
    providers.add(SessionIdSvc.class);
    providers.add(RotationIdSvc.class);
    return providers;
  }
}
