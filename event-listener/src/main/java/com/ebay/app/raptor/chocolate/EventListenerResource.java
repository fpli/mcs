package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.error.LocalizedErrorFactoryV3;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageIdEnum;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContextProvider;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import com.ebay.raptor.opentracing.SpanEventHelper;
import com.ebay.raptor.opentracing.Tags;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
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

  private Metrics metrics;

  /**
   * Initialize function
   */
  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  /**
   * Collect marketing tracking event.
   * @param body json body of the unified tracking schema event.
   *             It does not contain all columns cause some of them can be parsed in the collection service.
   * @return Response telling it's successful or not
   */
  @Override
  public Response track(UnifiedTrackingEvent body) {
    Tracer tracer = GlobalTracer.get();
    try (Scope scope = tracer.buildSpan("mktcollectionsvc").withTag(Tags.TYPE.getKey(), "UnifiedTracking")
        .startActive(true)) {
      Span span = scope.span();
      Response res = null;
      try {
        // click events
        collectionService.collectUnifiedTrackingEvent(body);
        res = Response.status(Response.Status.CREATED).build();
        Tags.STATUS.set(span, "0");
      } catch (Exception e) {
        Tags.STATUS.set(span, "0");
        // show warning in cal
        SpanEventHelper.writeEvent("Warning", "mktcollectionsvc", "1", e.getMessage());
        try {
          res = errorFactoryV3.makeWarnResponse(e.getMessage());
        } catch (Exception ex) {
          logger.warn(e.getMessage(), request.toString(), body);
          logger.warn(ex.getMessage(), ex);
        }
      }
      return res;
    }
  }

  /**
   * Collect clicks from upstream raptor, nodejs and native app
   * @param body json body containing referrerUrl and targetUrl
   * @return Response telling it's successful or not
   */
  @Override
  public Response event(Event body) {
    Tracer tracer = GlobalTracer.get();
    try(Scope scope = tracer.buildSpan("mktcollectionsvc").withTag(Tags.TYPE.getKey(), "event").startActive(true)) {
      Span span = scope.span();
      Response res = null;
      try {
        // click events
        collectionService.collect(request, userCtxProvider.get(), raptorSecureContextProvider.get(),
                requestContext, body);

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
      }
      return res;
    }
  }

  @Override
  public Response getVersion() {
    return Response.ok("1.0").build();
  }

  /**
   * Get method to collect impression, viewimp, email open, ad request
   * @return response
   */
  @Override
  public Response impression(Event body) {
    Tracer tracer = GlobalTracer.get();
    try(Scope scope = tracer.buildSpan("mktcollectionsvc").withTag(Tags.TYPE.getKey(), "impression")
        .startActive(true)) {
      Span span = scope.span();
      Response res = null;
      try {
        collectionService.collectImpression(request, userCtxProvider.get(), raptorSecureContextProvider.get(),
            requestContext, body);
        res = Response.status(Response.Status.OK).build();
        Tags.STATUS.set(span, "0");
      } catch (Exception e) {
        // logger.warn(e.getMessage(), e);
        // Tags.STATUS.set(span, e.getMessage());
        Tags.STATUS.set(span, "0");
        // show warning in cal
        SpanEventHelper.writeEvent("Warning", "mktcollectionsvc", "1", e.getMessage());
        try {
          res = errorFactoryV3.makeWarnResponse(e.getMessage());
        } catch (Exception ex) {
          logger.warn(ex.getMessage(), ex);
        }
      }
      return res;
    }
  }

  @Override
  public Response roi(ROIEvent body) {
    Tracer tracer = GlobalTracer.get();
    try(Scope scope = tracer.buildSpan("mktcollectionsvc").withTag(Tags.TYPE.getKey(), "roi").startActive(true)) {
      Span span = scope.span();
      Response res = null;
      try {
        collectionService.collectROIEvent(request, userCtxProvider.get(), raptorSecureContextProvider.get(), requestContext, body);
        res = Response.status(Response.Status.CREATED).build();
        Tags.STATUS.set(span, "0");
      } catch (Exception e) {
        Tags.STATUS.set(span, "0");
        // show warning in cal
        SpanEventHelper.writeEvent("Warning", "mktcollectionsvc", "1", e.getMessage());
        try {
          res = errorFactoryV3.makeWarnResponse(e.getMessage());
        } catch (Exception ex) {
          logger.warn(ex.getMessage(), ex);
        }
      }
      return res;
    }
  }

  /**
   * Collect sync events from adservice
   * @param body event body
   * @return response
   */
  @Override
  public Response sync(Event body) {
    Tracer tracer = GlobalTracer.get();
    try (Scope scope = tracer.buildSpan("mktcollectionsvc").withTag(Tags.TYPE.getKey(), "sync").startActive(true)) {
      Span span = scope.span();
      Response res = null;
      try {
        collectionService.collectSync(request, raptorSecureContextProvider.get(), requestContext, body);
        res = Response.status(Response.Status.CREATED).build();
        Tags.STATUS.set(span, "0");
      } catch (Exception e) {
        Tags.STATUS.set(span, "0");
        // show warning in cal
        SpanEventHelper.writeEvent("Warning", "mktcollectionsvc", "1", e.getMessage());
        try {
          res = errorFactoryV3.makeWarnResponse(e.getMessage());
        } catch (Exception ex) {
          logger.warn(e.getMessage(), request.toString(), body);
          logger.warn(ex.getMessage(), ex);
        }
      }
      return res;
    }
  }
}


