package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.error.LocalizedErrorFactoryV3;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContextProvider;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import com.ebay.raptor.opentracing.SpanEventHelper;
import com.ebay.raptor.opentracing.Tags;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
  private static final Logger logger = LoggerFactory.getLogger(EventListenerResource.class);
  @Autowired
  private CollectionService collectionService;

  @Autowired
  private LocalizedErrorFactoryV3 errorFactoryV3;

  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

  @Autowired
  private IEndUserContextProvider userCtxProvider;

  @Autowired
  private RaptorSecureContextProvider raptorSecureContextProvider;

  @Context
  private ContainerRequestContext requestContext;

  @Override
  public Response event(Event body) {
    Tracer tracer = GlobalTracer.get();
    try(Scope scope = tracer.buildSpan("mktcollectionsvc").withTag(Tags.TYPE.getKey(), "event").startActive(true)) {
      Span span = scope.span();
      Response res = null;
      try {
        collectionService.collect(request, userCtxProvider.get(), raptorSecureContextProvider.get(), requestContext, body);
        res = Response.status(Response.Status.CREATED).build();
        Tags.STATUS.set(span, "0");
      } catch (Exception e) {
        // do not write log here for short term. As current native app sends seo deeplinking which leads log blast.
        //logger.warn(e.getMessage(), e);
        // Tags.STATUS.set(span, e.getMessage());
        Tags.STATUS.set(span, "0");
        // show warning in cal
        SpanEventHelper.writeEvent("Warning", "mktcollectionsvc", "1", e.getMessage());
        try {
          res = errorFactoryV3.makeWarnResponse(e.getMessage());
        } catch (Exception ex) {
          logger.warn(e.getMessage(), request.toString(), body);
          logger.warn(ex.getMessage(), ex);
        }
      } finally {
        return res;
      }
    }
  }

  @Override
  public Response getVersion() {
    return Response.ok("1.0").build();
  }

  @Override
  public Response impression() {
    Tracer tracer = GlobalTracer.get();
    try(Scope scope = tracer.buildSpan("mktcollectionsvc").withTag(Tags.TYPE.getKey(), "impression").startActive(true)) {
      Span span = scope.span();
      Response res = null;
      try {
        collectionService.collectImpression(request, response, requestContext);
        res = Response.status(Response.Status.CREATED).build();
        Tags.STATUS.set(span, "0");
      } catch (Exception e) {
        // do not write log here for short term. As current native app sends seo deeplinking which leads log blast.
        //logger.warn(e.getMessage(), e);
        // Tags.STATUS.set(span, e.getMessage());
        Tags.STATUS.set(span, "0");
        // show warning in cal
        SpanEventHelper.writeEvent("Warning", "mktcollectionsvc", "1", e.getMessage());
        try {
          res = errorFactoryV3.makeWarnResponse(e.getMessage());
        } catch (Exception ex) {
          logger.warn(e.getMessage(), request.toString());
          logger.warn(ex.getMessage(), ex);
        }
      } finally {
        return res;
      }
    }
  }
}


