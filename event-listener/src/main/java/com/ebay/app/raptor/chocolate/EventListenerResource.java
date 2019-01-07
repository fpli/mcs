package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.error.LocalizedErrorFactoryV3;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContextProvider;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import com.ebay.raptor.opentracing.Tags;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.springframework.beans.factory.annotation.Autowired;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 * Resource class
 *
 * @author xiangli4
 */

@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
public class EventListenerResource implements EventsApi {

  @Autowired
  private CollectionService collectionService;

  @Autowired
  private LocalizedErrorFactoryV3 errorFactoryV3;

  @Autowired
  private HttpServletRequest request;

  @Autowired
  private IEndUserContextProvider userCtxProvider;

  @Autowired
  private RaptorSecureContextProvider raptorSecureContextProvider;

  @Context
  private ContainerRequestContext requestContext;

  @Override
  public Response event(Event body) {
    Tracer tracer = GlobalTracer.get();
    try(Scope scope = tracer.buildSpan("mktCollectionSvc").withTag(Tags.TYPE.getKey(), "URL").startActive(true)) {
      Span span = scope.span();
      Response res;
      try {
        boolean result = collectionService.collect(request, userCtxProvider.get(), raptorSecureContextProvider.get(), requestContext, body);
        res = Response.status(Response.Status.CREATED).build();
        Tags.STATUS.set(span, "0");
        return res;
      } catch (Exception e) {
        Tags.STATUS.set(span, e.getClass().getSimpleName());
        throw errorFactoryV3.makeException(e.getMessage());
      }
    }
  }

  @Override
  public Response getVersion() {
    return Response.ok("1.0").build();
  }
}


