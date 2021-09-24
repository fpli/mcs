package com.ebay.app.raptor.chocolate.filter.lbs;

import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.traffic.monitoring.ESMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;

public class LBSClientTest {

    @Before
    public void setUp(){
        ESMetrics.init("test", "localhost");
        Client client = Mockito.mock(Client.class);
        Configuration conf = Mockito.mock(Configuration.class);
        Response response = prepareResponse();
        WebTarget webTarget = Mockito.mock(WebTarget.class);
        Invocation.Builder builder = Mockito.mock(Invocation.Builder.class);

        Mockito.when(conf.getProperty(EndpointUri.KEY)).thenReturn("localhost");
        Mockito.when(builder.get()).thenReturn(response);
        Mockito.when(webTarget.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        Mockito.when(client.target(anyString())).thenReturn(webTarget);
        Mockito.when(client.getConfiguration()).thenReturn(conf);

        LBSClient.init(client);
    }

    @Test
    public void testGetIpFromHeader() {
        assertEquals(76137L, LBSClient.getInstance().getPostalCodeByIp("155.94.176.242"));
    }

    private Response prepareResponse() {
        LBSQueryResult queryResult = new LBSQueryResult();
        queryResult.setRegionCode("44");
        queryResult.setPostalCode("76137");
        queryResult.setMetroCode("623");
        queryResult.setIsoCountryCode2("US");
        queryResult.setStateCode("TX");
        queryResult.setLongitude(-97.2934d);
        queryResult.setAreaCodes("817/682");
        queryResult.setLatitude(32.86d);
        queryResult.setQueryId("chocolate_geotargeting_ip_1");
        queryResult.setCity("ft worth");

        assertEquals("44", queryResult.getRegionCode());
        assertEquals("623", queryResult.getMetroCode());
        assertEquals("US", queryResult.getIsoCountryCode2());
        assertEquals("TX", queryResult.getStateCode());
        assertEquals(-97.2934d, queryResult.getLongitude(), 0);
        assertEquals("817", queryResult.getAreaCodes());
        assertEquals(32.86d, queryResult.getLatitude(), 0);
        assertEquals("chocolate_geotargeting_ip_1", queryResult.getQueryId());
        assertEquals("ft worth", queryResult.getCity());

        List<LBSQueryResult> queryResultList = new ArrayList<>();
        queryResultList.add(queryResult);

        LBSHttpResult httpResult = new LBSHttpResult();
        httpResult.setHttpStatus(200);
        httpResult.setQueryResult(queryResultList);

        assertEquals(200, httpResult.getHttpStatus());

        List<LBSHttpResult> httpResultList = new ArrayList<>();
        httpResultList.add(httpResult);

        LBSResults lbsResults = new LBSResults();
        lbsResults.setAllResults(httpResultList);

        Response response = Mockito.mock(Response.class);
        Mockito.when(response.getStatus()).thenReturn(200);
        Mockito.when(response.readEntity(LBSResults.class)).thenReturn(lbsResults);
        return response;
    }

}