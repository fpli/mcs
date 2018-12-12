package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.util.Constants;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.ErrorData;
import com.ebay.app.raptor.chocolate.gen.model.ErrorModel;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContextProvider;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import com.ebay.raptor.opentracing.Tags;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


/**
 * Template resource class
 *
 * @author xiangli4
 */

@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
public class EventListenerResource implements EventsApi {
  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

  @Autowired
  private IEndUserContextProvider userCtxProvider;

  @Autowired
  RaptorSecureContextProvider raptorSecureContextProvider;

  /**
   * Generate error response
   * @param errorMsg
   * @return
   */
  public ErrorModel makeBadRequestError(String errorMsg) {
    ErrorData detail = new ErrorData();
    detail.setErrorCode(new BigDecimal(Constants.errorMessageMap.get(errorMsg)));
    detail.setCategory(ErrorData.CategoryEnum.REQUEST);
    detail.setDomain(ErrorData.DomainEnum.MARKETING);
    detail.setSubdomain(ErrorData.SubdomainEnum.TRACKING);
    detail.setMessage("Invalid Request!");
    detail.setLongMessage(errorMsg);
    ErrorModel errorModel = new ErrorModel();
    List<ErrorData> errors = new ArrayList<>();
    errors.add(detail);
    errorModel.setErrors(errors);

    return errorModel;
  }

  @Override
  public Response event(Event body, String contentType, String userAgent, String X_EBAY_C_ENDUSERCTX, String
    X_EBAY_C_TRACKING_REF, String referrer) {

    Tracer tracer = GlobalTracer.get();
    try(Scope scope = tracer.buildSpan("mktCollectionSvc").withTag(Tags.TYPE.getKey(), "URL").startActive(true)){
      Span span = scope.span();
      Response res;
      try {
        String result = CollectionService.getInstance().collect(request, userCtxProvider.get(), raptorSecureContextProvider.get(), body);
        if (result.equals(Constants.ACCEPTED)) {
          res = Response.ok().entity(result).build();
        } else {
          res = Response.status(Response.Status.BAD_REQUEST).entity(makeBadRequestError(result)).build();
        }
        Tags.STATUS.set(span, "0");
      } catch (Exception e) {
        Tags.STATUS.set(span, e.getClass().getSimpleName());
        res = Response.status(Response.Status.BAD_REQUEST).entity(makeBadRequestError(Constants.ERROR_INTERNAL_SERVICE)).build();
      }
      return res;
    }
  }
}


