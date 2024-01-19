package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.ARReportService;
import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

/**
 * Created by jialili1 on 1/18/24
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ginger-client.testService.testClient.endpointUri=http://localhost",
                "ginger-client.testService.testClient.connectTimeout=60000",
                "ginger-client.testService.testClient.readTimeout=60000"
        },
        classes = AdserviceApplication.class)
public class ARResourceTest {

    @LocalServerPort
    private int port;

    private boolean initialized = false;

    private Client client;
    private String svcEndPoint;

    private static final String ATTESTATION_PATH = "/.well-known/privacy-sandbox-attestations.json";

    @Autowired
    private ARReportService arReportService;

    @BeforeClass
    public static void initBeforeTest() {
        ApplicationOptions options = ApplicationOptions.getInstance();
    }

    @Before
    public void setUp() {
        if (!initialized) {
            RuntimeContext.setConfigRoot(AdserviceResourceTest.class.getClassLoader().getResource
                    ("META-INF/configuration/Dev/"));
            Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
            client = ClientBuilder.newClient(configuration);
            String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
            svcEndPoint = endpoint + ":" + port;

            initialized = true;
        }
    }

    private Response getARResponse(String path) {
        return client.target(svcEndPoint).path(path).request().accept(MediaType.APPLICATION_JSON_TYPE).get();
    }

    @Test
    public void attestation() {
        Response response = getARResponse(ATTESTATION_PATH);
        assertEquals(200, response.getStatus());
    }
}
