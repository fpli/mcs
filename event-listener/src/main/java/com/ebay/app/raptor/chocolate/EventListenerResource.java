package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.util.Constants;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.ErrorData;
import com.ebay.app.raptor.chocolate.gen.model.ErrorModel;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.kernel.calwrapper.CalTransaction;
import com.ebay.raptor.calclient.api.ICalTransactionFactory;
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
@Produces(MediaType.APPLICATION_JSON)
public class EventListenerResource implements EventsApi {
  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

  @Autowired
  ICalTransactionFactory calTransactionFactory;

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
    CalTransaction calTransaction = calTransactionFactory.create("MarketingCollectionService");
    try {
      calTransaction.setName("events");
      String result = CollectionService.getInstance().collect(request, body);

      if (result.equals(Constants.ACCEPTED)) {
        return Response.ok().entity(result).build();
      } else {
        return Response.status(Response.Status.BAD_REQUEST).entity(makeBadRequestError(result)).build();
      }
    }
    finally {
      calTransaction.setStatus("0");
      calTransaction.completed();
    }
  }
}


