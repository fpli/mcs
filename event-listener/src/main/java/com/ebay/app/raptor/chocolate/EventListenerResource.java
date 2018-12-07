package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.util.Constants;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.ErrorData;
import com.ebay.app.raptor.chocolate.gen.model.ErrorModel;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.kernel.calwrapper.CalTransaction;
import com.ebay.kernel.calwrapper.CalTransactionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
@Component
@Consumes(MediaType.APPLICATION_JSON)
public class EventListenerResource implements EventsApi {
  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

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
    CalTransaction calTransaction = CalTransactionFactory.create("URL");
    calTransaction.setName("MktCollectionSvc");
    Response res;
    try {
      String result = CollectionService.getInstance().collect(request, body);
      if (result.equals(Constants.ACCEPTED)) {
        res = Response.ok().entity(result).build();
      } else {
        res = Response.status(Response.Status.BAD_REQUEST).entity(makeBadRequestError(result)).build();
      }
      calTransaction.setStatus("0");
    } catch (Exception e) {
      calTransaction.setStatus(e);
      res = Response.status(Response.Status.BAD_REQUEST).entity(makeBadRequestError(Constants.ERROR_INTERNAL_SERVICE)).build();
    }
    finally {
      calTransaction.completed();
    }
    return res;
  }
}


