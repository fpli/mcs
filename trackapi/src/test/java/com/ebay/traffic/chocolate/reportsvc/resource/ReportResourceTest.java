package com.ebay.traffic.chocolate.reportsvc.resource;

import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.traffic.chocolate.mkttracksvc.MkttracksvcApplication;
import com.ebay.traffic.chocolate.mkttracksvc.dao.CouchbaseClient;
import com.ebay.traffic.chocolate.reportsvc.TestHelper;
import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import com.ebay.traffic.chocolate.reportsvc.constant.Granularity;
import com.ebay.traffic.chocolate.reportsvc.constant.ReportType;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ginger-client.testService.testClient.endpointUri=http://localhost",
                "ginger-client.testService.testClient.readTimeout=60000"
        },
        classes = MkttracksvcApplication.class)
@Ignore("integrity")
public class ReportResourceTest {

  @LocalServerPort
  private int port;

  @Autowired
  private CouchbaseClient couchbaseClient;

  private boolean initialized = false;

  private Client client;
  private String svcEndPoint;

  private static final String REPORT_PATH = "/tracksvc/v1/rpt/report";

  @Before
  public void init() {
    // port and couchbaseClient are injected as non-static member, so can't use @BeforeClass.
    if (!initialized) {
      Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
      client = ClientBuilder.newClient(configuration);
      String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
      svcEndPoint = endpoint + ":" + port;

      // Generate test data...
      TestHelper.prepareTestData(couchbaseClient.getReportBucket(), new String[]{"PUBLISHER_11", "PUBLISHER_11_CAMPAIGN_22"});

      initialized = true;
    }
  }

  @Test
  public void testGenerateReportWithPartnerId() {
    Response result = client.target(svcEndPoint).path(REPORT_PATH)
            .queryParam("partnerid", "11")
            .queryParam("daterange", "custom")
            .queryParam("startdate", "2000-01-01")
            .queryParam("enddate", "2000-01-02")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get();

    Assert.assertEquals(result.getStatus(), 200);

    ReportResponse response = result.readEntity(ReportResponse.class);
    ReportRequest request = response.getRequest();

    Assert.assertEquals("PUBLISHER_11", request.getKeyPrefix());
    Assert.assertEquals(20000101, request.getStartDate());
    Assert.assertEquals(20000102, request.getEndDate());
    Assert.assertEquals(DateRange.CUSTOM, request.getDateRange());
    Assert.assertEquals(ReportType.PARTNER, request.getReportType());
    Assert.assertEquals(Granularity.DAY, request.getGranularity());
  }

  @Test
  public void testGenerateReportWithCampaignId() {
    Response result = client.target(svcEndPoint).path(REPORT_PATH)
            .queryParam("partnerid", "11")
            .queryParam("campaignid", "22")
            .queryParam("daterange", "custom")
            .queryParam("startdate", "2000-01-01")
            .queryParam("enddate", "2000-01-02")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get();

    Assert.assertEquals(result.getStatus(), 200);

    ReportResponse response = result.readEntity(ReportResponse.class);
    ReportRequest request = response.getRequest();

    Assert.assertEquals("PUBLISHER_11_CAMPAIGN_22", request.getKeyPrefix());
    Assert.assertEquals(20000101, request.getStartDate());
    Assert.assertEquals(20000102, request.getEndDate());
    Assert.assertEquals(DateRange.CUSTOM, request.getDateRange());
    Assert.assertEquals(ReportType.CAMPAIGN, request.getReportType());
    Assert.assertEquals(Granularity.DAY, request.getGranularity());
  }

}
