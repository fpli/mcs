package chocolate;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.eventlistener.constant.ErrorType;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.cos.raptor.error.v3.ErrorMessageV3;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.cosadaptor.token.SecureTokenFactory;
import com.ebay.platform.raptor.ddsmodels.AppInfo;
import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.test.framework.RaptorIOSpringRunner;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.ListenerMessageDeserializer;
import com.ebay.traffic.chocolate.kafka.ListenerMessageSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.ebay.traffic.chocolate.common.TestHelper.pollFromKafkaTopic;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doAnswer;

/**
 * Created by xiangli4 on 11/19/18.
 * End to End test for Event Listener Service. This class uses Spring test framework to
 * start the test web service, and uses Mini Kafka.
 */
@RunWith(RaptorIOSpringRunner.class)
@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = {
    "ginger-client.testService.testClient.endpointUri=http://localhost",
    "ginger-client.testService.testClient.readTimeout=60000"
  },
  classes = EventListenerApplication.class)
public class EventListenerServiceTest {
  private static MiniKafkaCluster kafkaCluster;

  @LocalServerPort
  private int port;

  private boolean initialized = false;

  private Client client;
  private String svcEndPoint;

  private final String path = "/marketingtracking/v1/events";

  @Autowired
  private CollectionService collectionService;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void initBeforeTest() throws Exception {
    // inject kafka properties before springboot start
    kafkaCluster = KafkaTestHelper.newKafkaCluster();
    ApplicationOptions options = ApplicationOptions.getInstance();
    options.setSinkKafkaProperties(kafkaCluster.getProducerProperties(
      LongSerializer.class, ListenerMessageSerializer.class));
  }

  @Before
  public void setUp() {
    if (!initialized) {
      RuntimeContext.setConfigRoot(EventListenerServiceTest.class.getClassLoader().getResource
        ("META-INF/configuration/Dev/"));
      Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
      client = ClientBuilder.newClient(configuration);
      String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
      svcEndPoint = endpoint + ":" + port;

      prepareData();
      initialized = true;
    }
  }

  private static void prepareData() {

  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  @Test
  public void testResource() throws Exception {

    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&cid=2");

    String endUserCtxiPhone = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
      ".com%2Fdisplay%2FTRACKING%2FTest%2BMarketing%2Btracking,uri=%2Fsampleappweb%2Fsctest," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";

    String endUserCtxAndroid = "ip=10.148.184.210,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml" +
      "%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8," +
      "userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=ebayUserAgent%2FeBayAndroid%3B5.27.1%3BAndroid%3B8.0.0%3Bsamsung%3Bgreatqlte%3BU.S" +
      ".%20Cellular%3B1080x2094%3B2.6,deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
      ".com%2Fdisplay%2FTRACKING%2FTest%2BMarketing%2Btracking,uri=%2Fsampleappweb%2Fsctest," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";

    String endUserCtxDesktop = "ip=10.148.184.205," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=Mozilla%2F5.0+%28Macintosh%3B+Intel+Mac+OS+X+10_13_6%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko" +
      "%29+Chrome%2F71.0.3578.98+Safari%2F537.36,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
      ".com%2Fdisplay%2FtrafficCOE%2FLong%2Bterm%2Bstage1%253A%2BTrack%2Bclick%2Bfrom%2Blanding%2Bpage," +
      "xff=10.249.74.17,uri=%2Fmkttestappweb%2Fi%2FApple-iPhone-8-Plus-256gb-Gold%2F290016063137," +
      "applicationURL=http%3A%2F%2Fmkttestapp.stratus.qa.ebay" +
      ".com%2Fmkttestappweb%2Fi%2FApple-iPhone-8-Plus-256gb-Gold%2F290016063137%3Fmkevt%3D1%26cid%3D2," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true";

    String endUserCtxMweb = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=Mozilla%2F5.0%20%28iPhone%3B%20CPU%20iPhone%20OS%2012_1_2%20like%20Mac%20OS%20X%29%20AppleWebKit" +
      "%2F605.1.15%20%28KHTML%2C%20like%20Gecko%29%20Version%2F12.0%20Mobile%2F15E148%20Safari%2F604.1," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
      ".com%2Fdisplay%2FTRACKING%2FTest%2BMarketing%2Btracking,uri=%2Fsampleappweb%2Fsctest," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";

    String endUserCtxNoReferer = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";

    String tracking = "guid=8101a7ad1670ac3c41a87509fffc40b4,cguid=8101b2b31670ac797944836ecffb525d," +
      "tguid=8101a7ad1670ac3c41a87509fffc40b4,cobrandId=2";
    // success request
    // iphone
    Response response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // desktop
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxDesktop)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // android
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxAndroid)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // mweb
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxMweb)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-ENDUSERCTX
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(400, response.getStatus());
    ErrorMessageV3 errorMessageV3 = response.readEntity(ErrorMessageV3.class);
    assertEquals(4001, errorMessageV3.getErrors().get(0).getErrorId());

    // no X-EBAY-C-TRACKING
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(400, response.getStatus());
    errorMessageV3 = response.readEntity(ErrorMessageV3.class);
    assertEquals(4002, errorMessageV3.getErrors().get(0).getErrorId());

    // no referer
    event.setReferrer(null);
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(400, response.getStatus());
    errorMessageV3 = response.readEntity(ErrorMessageV3.class);
    assertEquals(4003, errorMessageV3.getErrors().get(0).getErrorId());

    // no query parameter
    event.setTargetUrl("https://www.ebay.com");
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(400, response.getStatus());
    errorMessageV3 = response.readEntity(ErrorMessageV3.class);
    assertEquals(4005, errorMessageV3.getErrors().get(0).getErrorId());

    // no mkevt
    event.setTargetUrl("https://www.ebay.com?cid=2");
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(400, response.getStatus());
    errorMessageV3 = response.readEntity(ErrorMessageV3.class);
    assertEquals(4006, errorMessageV3.getErrors().get(0).getErrorId());

    // invalid mkevt
    event.setTargetUrl("https://www.ebay.com?cid=2&mkevt=0");
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(400, response.getStatus());
    errorMessageV3 = response.readEntity(ErrorMessageV3.class);
    assertEquals(4007, errorMessageV3.getErrors().get(0).getErrorId());

    // no cid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkevt=1");
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // invalid cid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?cid=99&mkevt=1");
    response = client.target(svcEndPoint).path(path)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
      consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 4, 30 * 1000);
    consumerPaidSearch.close();

    assertEquals(4, listenerMessagesPaidSearch.size());
  }
}
