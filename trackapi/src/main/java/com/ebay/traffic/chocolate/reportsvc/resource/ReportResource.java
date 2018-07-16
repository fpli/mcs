package com.ebay.traffic.chocolate.reportsvc.resource;

import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportResponse;
import com.ebay.traffic.chocolate.reportsvc.service.ReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

@Component
@Path("/rpt")
public class ReportResource {

  @Autowired
  private ReportService reportService;

  @GET
  @Path("/report")
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
  @Produces(MediaType.APPLICATION_JSON)
  public ReportResponse generateReport(
          @QueryParam(value = "partnerid") final String partnerId,
          @QueryParam(value = "campaignid") final String campaignId,
          @QueryParam(value = "daterange") final String dateRange,
          @QueryParam(value = "startdate") final String startDate,
          @QueryParam(value = "enddate") final String endDate) {

    Map<String, String> incomingRequest = new HashMap<String, String>() {
      {
        put("partnerId", partnerId);
        put("campaignId", campaignId);
        put("dateRange", dateRange);
        put("startDate", startDate);
        put("endDate", endDate);
      }
    };

    ReportRequest request = null;
    try {
      request = new ReportRequest(incomingRequest);
    } catch (Exception e) {

    }

    try {
      return reportService.generateReportForRequest(request);
    } catch (Throwable t) {
    }

    return null;
  }
}
