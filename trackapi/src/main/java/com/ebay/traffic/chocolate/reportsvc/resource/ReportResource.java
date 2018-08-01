package com.ebay.traffic.chocolate.reportsvc.resource;

import com.ebay.cos.raptor.service.annotations.*;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.ebay.traffic.chocolate.reportsvc.constant.ErrorType;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportResponse;
import com.ebay.traffic.chocolate.reportsvc.error.LocalizedErrorFactoryV3;
import com.ebay.traffic.chocolate.reportsvc.service.ReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

import static com.ebay.traffic.chocolate.reportsvc.constant.Errors.*;

@ApiRef(api = "marketing/tracking", version = "1")
@Component
@Path("/rpt")
public class ReportResource {

  @Autowired
  private LocalizedErrorFactoryV3 errorFactoryV3;

  @Autowired
  private ReportService reportService;

  private final ESMetrics esMetrics = ESMetrics.getInstance();

  @GET
  @Path("/report")
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
  @Produces(MediaType.APPLICATION_JSON)
  //@PreAuthorize("hasRole('https://api.ebay.com/oauth/scope/core@application')")
  @ApiMethod(name = "generateReport", resource = "report")
  @ApiDescription("The Api to generate reports for a publisher or campaign, if campaignId is provided")
  @ApiResponse(
          headers = {
                  @ApiHeader(header = HttpHeaders.LOCATION, description = "URL location of the partner report resource.")
          },
          codes = {
                  @ApiStatus(code = 200, description = "Generated"),
                  @ApiStatus(code = 400, description = "Bad Request"),
                  @ApiStatus(code = 403, description = "Unauthorized"),
                  @ApiStatus(code = 500, description = "Internal Server Error")
          },
          errors = {
                  @ApiError(errorId = 401003, description = INTERNAL_ERROR_MSG, domain = ERROR_DOMAIN, category = "APPLICATION",
                          inputRefIdsSupported = false, outputRefIdsSupported = false),
                  @ApiError(errorId = 401004, description = BAD_PARTNER_MSG, domain = ERROR_DOMAIN, category = "REQUEST",
                          inputRefIdsSupported = false, outputRefIdsSupported = false),
                  @ApiError(errorId = 401005, description = BAD_CAMPAIGN_MSG, domain = ERROR_DOMAIN, category = "REQUEST",
                          inputRefIdsSupported = false, outputRefIdsSupported = false),
                  @ApiError(errorId = 401006, description = BAD_DATES_MSG, domain = ERROR_DOMAIN, category = "REQUEST",
                            inputRefIdsSupported = false, outputRefIdsSupported = false),
                  @ApiError(errorId = 401007, description = EXPIRED_MSG, domain = ERROR_DOMAIN, category = "REQUEST",
                          inputRefIdsSupported = false, outputRefIdsSupported = false),
                  @ApiError(errorId = 401008, description = UNAUTHORIZED_MSG, domain = ERROR_DOMAIN, category = "REQUEST",
                          inputRefIdsSupported = false, outputRefIdsSupported = false)
          }
  )
  @ApiSchema(schemaClass = ReportResponse.class)
  public ReportResponse generateReport(
          @QueryParam(value = "partnerid")
          @ApiDescription("Unique Identifier for Partner")
          @ApiFieldMeta(required = true)
          final String partnerId,

          @QueryParam(value = "campaignid")
          @ApiDescription("Optional campaign id parameter - if provided, generate report data for campaign.")
          @ApiFieldMeta(required = true)
          final String campaignId,

          @QueryParam(value = "daterange")
          @ApiDescription("Predefined date range to generate report.")
          @ApiFieldMeta(required = true)
          final String dateRange,

          @QueryParam(value = "startdate")
          @ApiDescription("Start date for custom date range to generate report - ISO format yyyy-MM-dd.")
          final String startDate,

          @QueryParam(value = "enddate")
          @ApiDescription("End date for custom date range to generate report - ISO format yyyy-MM-dd.")
          final String endDate) {

    Map<String, String> incomingRequest = new HashMap<String, String>() {
      private static final long serialVersionUID = 1L;

      {
        put("partnerId", partnerId);
        put("campaignId", campaignId);
        put("dateRange", dateRange);
        put("startDate", startDate);
        put("endDate", endDate);
      }
    };


    esMetrics.meter("ReportIncomingRequest");
    ReportRequest request = null;
    try {
      request = new ReportRequest(incomingRequest);
    } catch (Exception e) {
      throw errorFactoryV3.makeException(e.getMessage());
    }

    try {
      ReportResponse reportResponse = reportService.generateReportForRequest(request);
      esMetrics.meter("ReportSuccessRequest");
      return  reportResponse;
    } catch (Throwable t) {
      esMetrics.meter("ReportExceptions");
      throw errorFactoryV3.makeException(ErrorType.INTERNAL_SERVICE_ERROR.getErrorKey());
    }
  }
}
