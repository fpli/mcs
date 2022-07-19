package chocolate;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.eventlistener.constant.ErrorType;
import com.ebay.app.raptor.chocolate.eventlistener.util.CouchbaseClientV2;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.platform.raptor.cosadaptor.exceptions.TokenCreationException;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
import com.ebay.traffic.chocolate.common.KafkaTestHelper;
import com.ebay.traffic.chocolate.common.MiniKafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.ListenerMessageDeserializer;
import com.ebay.traffic.chocolate.kafka.ListenerMessageSerializer;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.generateQueryString;
import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.isLongNumeric;
import static com.ebay.traffic.chocolate.common.TestHelper.pollFromKafkaTopic;
import static org.junit.Assert.*;

/**
 * Created by xiangli4 on 11/19/18.
 * End to End test for Event Listener Service. This class uses Spring test framework to
 * start the test web service, and uses Mini Kafka.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = {
    "ginger-client.testService.testClient.endpointUri=http://localhost",
    "ginger-client.testService.testClient.readTimeout=60000"
  },
  classes = EventListenerApplication.class)
public class EventListenerServiceTest {
  private static MiniKafkaCluster kafkaCluster;
  @Autowired
  CouchbaseClientV2 couchbaseClientV2;
  @LocalServerPort
  private int port;

  @Inject
  private ISecureTokenManager tokenGenerator;

  private boolean initialized = false;

  private Client client;
  private String svcEndPoint;
  private String token;

  private static String eventsPath;
  private static String impressionPath;
  private static String roiPath;
  private static String versionPath;
  private static String syncPath;
  private static String trackPath;

  private static String endUserCtxiPhone;
  private static String endUserCtxAndroid;
  private static String endUserCtxDesktop;
  private static String endUserCtxMweb;
  private static String endUserCtxNoReferer;
  private static String endUserCtxCheckoutAPI;
  private static String endUserCtxPlaceOfferAPI;
  private static String endUserCtxAndroidNoReferer;

  private static String tracking;
  private static Pattern roversites;

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
  public void setUp() throws TokenCreationException {
    if (!initialized) {
      RuntimeContext.setConfigRoot(EventListenerServiceTest.class.getClassLoader().getResource
        ("META-INF/configuration/Dev/"));
      Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
      client = ClientBuilder.newClient(configuration);
      String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
      svcEndPoint = endpoint + ":" + port;
      token = tokenGenerator.getToken().getAccessToken();
      prepareData();
      initialized = true;
    }
  }

  private static void prepareData() {
    eventsPath = "/marketingtracking/v1/events";
    impressionPath = "/marketingtracking/v1/impression";
    roiPath = "/marketingtracking/v1/roi";
    versionPath = "/marketingtracking/v1/getVersion";
    syncPath = "/marketingtracking/v1/sync";
    trackPath = "/marketingtracking/v1/track";

    endUserCtxiPhone = "ip=10.148.184.210," +
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
    endUserCtxAndroid = "ip=10.148.184.210,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml" +
      "%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8," +
      "userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=ebayUserAgent%2FeBayAndroid%3B5.27.1%3BAndroid%3B8.0.0%3Bsamsung%3Bgreatqlte%3BU.S" +
      ".%20Cellular%3B1080x2094%3B2.6,deviceId=8101a7ad1670ac3c41a87509fffc40b4,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
      ".com%2Fdisplay%2FTRACKING%2FTest%2BMarketing%2Btracking,uri=%2Fsampleappweb%2Fsctest," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";
    endUserCtxDesktop = "ip=10.148.184.205," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=Mozilla%2F5.0+%28Macintosh%3B+Intel+Mac+OS+X+10_13_6%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko" +
      "%29+Chrome%2F71.0.3578.98+Safari%2F537.36,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
      ".com%2Fdisplay%2FtrafficCOE%2FLong%2Bterm%2Bstage1%253A%2BTrack%2Bclick%2Bfrom%2Blanding%2Bpage," +
      "xff=10.249.74.17,uri=%2Fmkttestappweb%2Fi%2FApple-iPhone-8-Plus-256gb-Gold%2F290016063137," +
      "applicationURL=http%3A%2F%2Fmkttestapp.stratus.qa.ebay" +
      ".com%2Fmkttestappweb%2Fi%2FApple-iPhone-8-Plus-256gb-Gold%2F290016063137%3Fmkevt%3D1%26mkcid%3D2," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true";
    endUserCtxMweb = "ip=10.148.184.210," +
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
    endUserCtxNoReferer = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";
    endUserCtxCheckoutAPI = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=checkoutApi," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,uri=%2Fsampleappweb%2Fsctest," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";
    endUserCtxPlaceOfferAPI = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=Mozilla%2F5.0%20%28compatible%3B%20Ebay%2FPlaceOfferAPI%29," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,uri=%2Fsampleappweb%2Fsctest," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";
    endUserCtxAndroidNoReferer = "ip=10.148.184.210,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml" +
            "%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8," +
            "userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
            "userAgent=ebayUserAgent%2FeBayAndroid%3B5.27.1%3BAndroid%3B8.0.0%3Bsamsung%3Bgreatqlte%3BU.S" +
            ".%20Cellular%3B1080x2094%3B2.6,deviceId=8101a7ad1670ac3c41a87509fffc40b4,deviceIdType=IDREF," +
            "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
            ".com%2Fdisplay%2FTRACKING%2FTest%2BMarketing%2Btracking,uri=%2Fsampleappweb%2Fsctest," +
            "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
            "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
            "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
            "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";


    tracking = "guid=8101a7ad1670ac3c41a87509fffc40b4,cguid=8101b2b31670ac797944836ecffb525d," +
      "tguid=8101a7ad1670ac3c41a87509fffc40b4,cobrandId=2";

    roversites = Pattern.compile(
            "^(http[s]?:\\/\\/)?rover\\.(qa\\.)?ebay\\.[\\w-.]+($|\\/.*)",
            Pattern.CASE_INSENSITIVE);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  private Response postMcsResponse(String path, String endUserCtx, String tracking, Event event) {
    // add headers
    Invocation.Builder builder = client.target(svcEndPoint).path(path).request();
    if (!StringUtils.isEmpty(endUserCtx)) {
      builder = builder.header("X-EBAY-C-ENDUSERCTX", endUserCtx);
    }
    if (!StringUtils.isEmpty(tracking)) {
      builder = builder.header("X-EBAY-C-TRACKING", tracking);
    }

    return builder.header("Authorization", token).accept(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(event));
  }

  private Response postMcsResponseWithExtraHeader(String path, String endUserCtx, String tracking, Event event,
                                                  String extraKey, String extraValue) {
    // add headers
    Invocation.Builder builder = client.target(svcEndPoint).path(path).request();
    if (!StringUtils.isEmpty(endUserCtx)) {
      builder = builder.header("X-EBAY-C-ENDUSERCTX", endUserCtx);
    }
    if (!StringUtils.isEmpty(tracking)) {
      builder = builder.header("X-EBAY-C-TRACKING", tracking);
    }
    builder = builder.header(extraKey, extraValue);

    return builder.header("Authorization", token).accept(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(event));
  }

  private Response postMcsResponse(String path, String endUserCtx, String tracking, int statusCode, Event event) {
    // add headers
    Invocation.Builder builder = client.target(svcEndPoint).path(path).request();
    if (!StringUtils.isEmpty(endUserCtx)) {
      builder = builder.header("X-EBAY-C-ENDUSERCTX", endUserCtx);
    }
    if (!StringUtils.isEmpty(tracking)) {
      builder = builder.header("X-EBAY-C-TRACKING", tracking);
    }

    builder.header(Constants.NODE_REDIRECTION_HEADER_NAME, statusCode);

    return builder.header("Authorization", token).accept(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(event));
  }

  private Response postMcsResponse(String path, UnifiedTrackingEvent event) {
    return client.target(svcEndPoint).path(path).request().header("Authorization", token).
        accept(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(event));
  }

  private Response postMcsResponseWithStatusCode(String path, String endUserCtx, String tracking, Event event, String marketingStatusCode) {
    // add headers
    Invocation.Builder builder = client.target(svcEndPoint).path(path).request();
    if (!StringUtils.isEmpty(endUserCtx)) {
      builder = builder.header("X-EBAY-C-ENDUSERCTX", endUserCtx);
    }
    if (!StringUtils.isEmpty(tracking)) {
      builder = builder.header("X-EBAY-C-TRACKING", tracking);
    }
    if (!StringUtils.isEmpty(marketingStatusCode)) {
      builder = builder.header("X-EBAY-TRACKING-MARKETING-STATUS-CODE", marketingStatusCode);
    }

    return builder.header("Authorization", token).accept(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(event));
  }

  @Test
  public void testEventsResource() throws InterruptedException {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=2");

    // success request
    // iphone
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // desktop
    response = postMcsResponse(eventsPath, endUserCtxDesktop, tracking, event);
    assertEquals(201, response.getStatus());

    // android
    response = postMcsResponse(eventsPath, endUserCtxAndroid, tracking, event);
    assertEquals(201, response.getStatus());

    // mweb
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, event);
    assertEquals(201, response.getStatus());

    // with statusCode header 200
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, 200, event);
    assertEquals(201, response.getStatus());

    // with statusCode header 301
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, 301, event);
    assertEquals(201, response.getStatus());

    // with statusCode header 404
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, 404, event);
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-ENDUSERCTX
    response = postMcsResponse(eventsPath, null, tracking, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4001, errorMessage.getErrorCode());

    // no X-EBAY-C-TRACKING
    response = postMcsResponse(eventsPath, endUserCtxiPhone, null, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());

    // no referer
    event.setReferrer(null);
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // rover as referer
    event.setReferrer("https://rover.ebay.com/rover/1/709-41886-4896-0/2?mpre=https%3A%2F%2Fwww.ebay.fr%2Fulk%2Fsch%2F%3F_nkw%3Dcamescope%2520jvc%26mkevt%3D1%26mkrid%3D709-41886-4896-0%26mkcid%3D2%26keyword%3Dcamescope%2520jvc%26crlp%3D285602751787_%26MT_ID%3D58%26geo_id%3D32296%26rlsatarget%3Dkwd-119986605%26adpos%3D6o2%26device%3Dc%26loc%3D9056144%26poi%3D%26abcId%3D463856%26cmpgn%3D189547424%26sitelnk%3D&keyword=camescope%20jvc&crlp=285602751787_&MT_ID=58&geo_id=32296&rlsatarget=kwd-119986605&adpos=6o2&device=c&loc=9056144&poi=&abcId=463856&cmpgn=189547424&sitelnk=&gclid=Cj0KCQjwtMvlBRDmARIsAEoQ8zSmXHKLMq9rnAokRQtw5FQcGflfnJiPbRndTX1OvNzgImj7sGgkemsaAtw9EALw_wcB");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // rover as referer but encoded
    event.setReferrer("https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-117182-37290-0%2F2%3Fmpre%3Dhttps%253A%252F%252Fwww.ebay.com%252Fi%252F153018234148%253Fchn%253Dps%2526var%253D452828802628%26itemid%3D452828802628_153018234148%26targetid%3D477790169505%26device%3Dc%26adtype%3Dpla%26googleloc%3D9060230%26poi%3D%26campaignid%3D1746988278%26adgroupid%3D71277061587%26rlsatarget%3Dpla-477790169505%26abcId%3D1139306%26merchantid%3D6296724%26gclid%3DCj0KCQjwkoDmBRCcARIsAG3xzl8lXd3bcaLMaJ8-zY1zD-COSGJrZj-CVOht-VqgWiCtPBy_hrl38HgaAu2AEALw_wcB%26srcrot%3D711-117182-37290-0%26rvr_id%3D1973157993841%26rvr_ts%3Dc1c229cc16a0aa42c5d2b84affc9e842");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // forward rover redirect
    event.setReferrer("https://rover.ebay.com/rover/");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // forward rover fail
    event.setReferrer("https://rover.ebay.com/");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // UFES redirection, don't forward to rover
    event.setReferrer("https://rover.ebay.com/rover/");
    event.setTargetUrl("https://www.ebay.com/sch/i.html?_nkw=first+home+decor&mkevt=1&mkcid=2&ufes_redirect=true");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // referer is ebay site, but page is in whitelist, don't reject
    event.setReferrer("https://www.ebay.com/");
    event.setTargetUrl("https://www.ebay.co.uk/cnt/ReplyToMessages?M2MContact&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=2&ul_noapp=true");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // no query parameter
    event.setReferrer("https://www.google.com");
    event.setTargetUrl("https://www.ebay.com");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // no mkevt
    event.setTargetUrl("https://www.ebay.com/?mkcid=2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("https://www.ebay.com/?mkcid=2&mkevt=0");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com/?mkevt=1");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkcid=99&mkevt=1");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // search engine free listings
    event.setTargetUrl("https://www.ebay.com?mkcid=28&mkevt=1");
    event.setReferrer("https://www.google.com");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // mkrvrid in url to set rvrid
    event.setTargetUrl("https://www.ebay.com?mkcid=28&mkevt=1&mkrvrid=201918172817");
    event.setReferrer("https://www.google.com");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
      consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 10, 30 * 1000);
    consumerPaidSearch.close();

    assertEquals(12, listenerMessagesPaidSearch.size());

    // mrkt email click events
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=8&mkpid=12&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&crd=20190801034425&segname=AD379737195_GBH_BBDBENNEWROW_20180813_ZK&ymmmid=1740915&ymsid=1495596781385&yminstc=7");
    response = postMcsResponseWithExtraHeader(eventsPath, endUserCtxiPhone, tracking, event, "X-Purpose", "preview");
    assertEquals(201, response.getStatus());

    // adobe click events
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=8&mkpid=14&emsid=0&id=h1d3e4e16,2d2cb515,2d03a0a1&segname=" +
        "SOP708_SG49&country=US&pu=hrtHY5sgRPq&crd=20200211040813&sojTags=adcampid%3Did%2Cadcamppu%3Dpu%2Ccrd%3Dcrd%2C" +
        "segname%3Dsegname&adobeParams=id,p1,p2,p3,p4&adcamp_landingpage=https%3A%2F%2Fwww.ebay.de%2Fdeals" +
        "&adcamp_locationsrc=adobe");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // site email click events
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=7&mkpid=0&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponseWithExtraHeader(eventsPath, endUserCtxiPhone, tracking, event, "X-Purpose", "preview");
    assertEquals(201, response.getStatus());

    // no partner
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=7&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // invalid partner
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=7&mkpid=999&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // marketing SMS click events
    event.setTargetUrl("https://www.ebay.com/i/1234123132?mkevt=1&mkcid=24&smsid=111&did=222");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // site SMS click events
    event.setTargetUrl("https://www.ebay.com/i/1234123132?mkevt=1&mkcid=25&smsid=111&did=222");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());
  }

  @Test
  public void testEPage() throws InterruptedException {
    Event event = new Event();
    event.setReferrer("https://pages.ebay.com/sitemap.html?mkevt=1&mkcid=7&mkpid=0&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    event.setTargetUrl("https://c.ebay.com/marketingtracking/v1/pixel?mkevt=1&mkcid=7&mkpid=0&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623&originalRef=https%3A%2F%2Fwww.google.com");

    // 1. CRM, not in paid search channel, won't get kafka message
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // 2. No referer
    event.setTargetUrl("https://c.ebay.com/marketingtracking/v1/pixel?mkevt=1&mkcid=7&mkpid=0&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // 3. IMK pass
    event.setReferrer("https://pages.qa.ebay.com/sitemap.html?mkcid=2&mkrid=710-123456-1234-6&mkevt=1");
    event.setTargetUrl("https://c.qa.ebay.com/marketingtracking/v1/pixel?mkcid=2&mkrid=710-123456-1234-6&mkevt=1&originalRef=https%3A%2F%2Fwww.google.com");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // 4. get target url from originalUrl parameter pass
    event.setReferrer("https://pages.qa.ebay.com/");
    event.setTargetUrl("https://c.qa.ebay.com/marketingtracking/v1/pixel?mkcid=2&mkrid=710-123456-1234-6&mkevt=1&originalRef=https%3A%2F%2Fwww.google.com&originalUrl=https%3A%2F%2Fpages.qa.ebay.com%2Fsitemap.html");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
      consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 2, 30 * 1000);
    consumerPaidSearch.close();
    assertEquals(2, listenerMessagesPaidSearch.size());
  }

  @Test
  public void testSelfService() {
    Event event = new Event();
    event.setTargetUrl("https://www.ebay.com/i/1234123132?mkevt=1&mkcid=25&smsid=111&self_service=1&self_service_id=123");
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // validate couchbase message
    assertEquals("https://www.ebay.com/i/1234123132?mkevt=1&mkcid=25&smsid=111&self_service=1&self_service_id=123",
        couchbaseClientV2.getSelfServiceUrl("123"));
  }

  @Test
  public void testNewROIEvent() throws Exception {
    String token = tokenGenerator.getToken().getAccessToken();
    // Test event cases
    ROIEvent event = new ROIEvent();
    event.setItemId("192658398245");
    Map<String, String> aaa = new HashMap<String, String>();
    aaa.put("ff1", "ss");
    event.setPayload(aaa);
    event.setTransType("BO-MobileApp@");
    event.setUniqueTransactionId("1677235978009");

    // No timestamp case
    ROIEvent event2 = new ROIEvent();
    event2.setItemId("192658398245");
    event2.setTransType("BO-MobileApp@");
    event2.setUniqueTransactionId("1677235978009");

    Response response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event2));
    assertEquals(201, response1.getStatus());

    // No item id
    event2.setItemId(null);
    event2.setTransactionTimestamp("1677235978009");
    event2.setTransType("BO-MobileApp@");
    event2.setUniqueTransactionId("1677235978009");
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event2));
    assertEquals(201, response1.getStatus());

    // no transId
    event2.setItemId("192658398245");
    event2.setTransactionTimestamp("1677235978009");
    event2.setTransType("BO-MobileApp@");
    event2.setUniqueTransactionId(null);
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event2));
    assertEquals(201, response1.getStatus());

    // no enduserctx
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event));
    assertEquals(200, response1.getStatus());

    // no tracking
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event));
    assertEquals(200, response1.getStatus());

    //invalid item id
    event.setItemId("abc");
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event));
    assertEquals(201, response1.getStatus());

    //assertEquals("", event.getItemId());

    //invalid timestamp
    event.setTransactionTimestamp("-12423232");
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event));
    assertEquals(201, response1.getStatus());

    //assertTrue(Long.valueOf(event.getTransactionTimestamp()) > 0);

    // invalid transid
    event.setUniqueTransactionId("-12312312");

    //assertEquals("", event.getUniqueTransactionId());

    //no referrer in header, header in payload encoded
    Map<String, String> payload = new HashedMap();
    payload.put("referrer", "https%3A%2F%2Fwww.google.com");
    event.setPayload(payload);
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event));
    assertEquals(201, response1.getStatus());

    // null referer in payload
    payload.put("referrer", "");
    response1 = client.target(svcEndPoint).path(roiPath)
        .request()
        .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
        .header("X-EBAY-C-TRACKING", tracking)
        .header("Authorization", token)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(event));
    assertEquals(201, response1.getStatus());

  }

  @Test
  public void testCollectionServiceUtil() throws UnsupportedEncodingException {
    // Test isLongNumeric
    String strNum1 = "11";
    String strNum2 = null;
    String strNum3 = "";
    String strNum4 = "aa01";
    assertTrue(isLongNumeric(strNum1));
    assertFalse(isLongNumeric(strNum2));
    assertFalse(isLongNumeric(strNum3));
    assertFalse(isLongNumeric(strNum4));

    // Test generateQueryString
    ROIEvent event = new ROIEvent();
    event.setItemId("52357723598250");
    Map<String, String> payload = new HashMap<String, String>();
    payload.put("siteId","2");
    payload.put("roisrc","2");
    payload.put("api","1");
    event.setPayload(payload);
    event.setTransType("BIN-FP");
    event.setUniqueTransactionId("324357529");
    event.setTransactionTimestamp("1581427339000");
    String localTimestamp = Long.toString(System.currentTimeMillis());
    String expectQuery = "tranType=BIN-FP&uniqueTransactionId=324357529&itemId=52357723598250&transactionTimestamp=1581427339000&siteId=2&mpuid=0;52357723598250;324357529&api=1&roisrc=2";
    assertEquals(expectQuery, generateQueryString(event, payload,localTimestamp, "0"));
  }

  @Test
  public void testImpressionResource() throws Exception {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=1");

    // success request
    // iphone
    Response response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // desktop
    response = postMcsResponse(impressionPath, endUserCtxDesktop, tracking, event);
    assertEquals(200, response.getStatus());

    // android
    response = postMcsResponse(impressionPath, endUserCtxAndroid, tracking, event);
    assertEquals(200, response.getStatus());

    // mweb
    response = postMcsResponse(impressionPath, endUserCtxMweb, tracking, event);
    assertEquals(200, response.getStatus());

    // no X-EBAY-C-TRACKING
    response = postMcsResponse(impressionPath, endUserCtxiPhone, null, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());

    // no referer
    event.setReferrer(null);
    response = postMcsResponse(impressionPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());

    // referrer encoded
    event.setReferrer("https%3A%2F%2Fwww.google.com");
    response = postMcsResponse(impressionPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());

    // no query parameter
    event.setReferrer("https://www.google.com");
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // no mkevt
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=1");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=1&mkevt=1");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4007, errorMessage.getErrorCode());

    // mkevt=3, 6
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=1&mkevt=3");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=1&mkevt=6");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=99&mkevt=2");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerEpn = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesEpn = pollFromKafkaTopic(
      consumerEpn, Arrays.asList("dev_listened-epn"), 4, 30 * 1000);
    consumerEpn.close();

    Map<Long, ListenerMessage> listenerMessagesEpnExcludeRover = listenerMessageExcludeRover(listenerMessagesEpn);
    assertEquals(8, listenerMessagesEpnExcludeRover.size());

    // mrkt email open events
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=8&mkpid=12&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&crd=20190801034425&segname=AD379737195_GBH_BBDBENNEWROW_20180813_ZK&ymmmid=1740915&ymsid=1495596781385&yminstc=7");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // site email open events
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=0&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // GCX email open events
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=29&mkdid=4&trkId=1234567&bu=43551630917");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // GCX email open events
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=30&mkdid=4&trkId=1234567&bu=43551630917");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // no partner
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=7&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());

    // invalid partner
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=999&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testVersion() {
    Response response = client.target(svcEndPoint).path(versionPath)
      .request()
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .header("Authentication", token)
      .get();
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testEPNResource() throws Exception {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=1&mksid=2345123");

    String trackingNoCguid = "guid=8101a7ad1670ac3c41a87509fffc40b4," +
      "tguid=8101a7ad1670ac3c41a87509fffc40b4,cobrandId=2";

    // success request
    // iphone
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // success request no cguid
    // iphone
    response = postMcsResponse(eventsPath, endUserCtxiPhone, trackingNoCguid, event);
    assertEquals(201, response.getStatus());
  }

  @Test
  public void testDeeplinkResource() throws Exception {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");

    // success request
    // iphone
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // success request (remove '/' after host in referrer parameter)
    event.setTargetUrl("ebay://link/?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link/?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // success request (remove '/' after ebay://link)
    event.setTargetUrl("ebay://link?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    //no target url in deeplink case
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link/?nav=item.view&id=143421740982");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    //invalid target url in deeplink case
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=http%3A%2F%2Frover.ebay.com%2Frover%2F1%2F710-53481-19255-0%2F1%3Fff3%3D2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link/?nav=item.view&id=143421740982&referrer=http%3A%2F%2Frover.ebay.com%2Frover%2F1%2F710-53481-19255-0%2F1%3Fff3%3D2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    // no query parameter
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2F");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2F");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // no mkevt
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D0%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D0%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26mkcid%3D99%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay:///?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26mkcid%3D99%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // custom uri with Chocolate parameters
    // success request (custom uri with valid Chocolate parameters)
    event.setTargetUrl("ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1&mkrid=710-53481-19255-0&campid=5337369893&toolid=11800&customid=chocolatetest&referrer=https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-53200-19255-0%2F1");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1&mkrid=710-53481-19255-0&campid=5337369893&toolid=11800&customid=chocolatetest&referrer=https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-53200-19255-0%2F1");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    //no valid tracking parameters in deeplink url
    event.setTargetUrl("ebay://link?nav=item.view&id=154347659933");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link?nav=item.view&id=154347659933");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("ebay://link?nav=item.view&id=154347659933&mkevt=1");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link?nav=item.view&id=154347659933&mkevt=1");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("ebay://link?nav=item.view&id=154347659933&mkevt=2&mkcid=1");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link?nav=item.view&id=154347659933&mkevt=2&mkcid=1");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=99");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=99");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    //deeplink can be tracked successfully
    event.setTargetUrl("ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=item.view&mkevt=1&mkcid=1&mkrid=710-53481-19255-0&campid=1234567");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=item.view&mkevt=1&mkcid=1&mkrid=710-53481-19255-0&campid=1234567");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    // no valid item id
    event.setTargetUrl("ebay://link?nav=item.view&id=&mkevt=1&mkcid=1&mkrid=710-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link?nav=item.view&id=&mkevt=1&mkcid=1&mkrid=710-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    // not valid item link
    event.setTargetUrl("ebay://link?nav=home&id=154347659933&mkevt=1&mkcid=1&mkrid=710-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link?nav=home&id=154347659933&mkevt=1&mkcid=1&mkrid=710-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    // invalid rotation
    event.setTargetUrl("ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1&mkrid=999-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    event.setTargetUrl("padebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1&mkrid=999-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4012, errorMessage.getErrorCode());

    // for ulk deep link
    event.setTargetUrl("ebay://link?nav=item.view&id=&mkevt=1&mkcid=4&mkrid=710-53481-19255-0&keyword=search&gclid=123");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=item.view&id=&mkevt=1&mkcid=4&mkrid=710-53481-19255-0&keyword=search&gclid=123");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=home&id=154347659933&mkevt=1&mkcid=4&mkrid=710-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&id=154347659933&mkevt=1&mkcid=4&mkrid=710-53481-19255-0");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38&campid=1234566&toolid=11001&customid=123");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38&campid=1234566&toolid=11001&customid=123");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38&campid=1234566&toolid=11001&customid=123");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38&campid=1234566&toolid=11001&customid=123");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38&campid=1234566&toolid=11001&customid=123");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38&campid=1234566&toolid=11001&customid=123");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=1&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=home&ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    event.setReferrer("https://www.ebay.fr/ulk/start/shop?ul_skipRefererCheck=true&ul_alt=store&sabg=355858d317a0a64695678467ffe56a8e&sabc=15f8667e17a0a8260c24e171f44d2fc7&mkevt=1&mkcid=4&mkrid=711-58542-18990-38");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=11923&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=abc&placement-type=abcd&keyword=bcd&gclid=123");
    event.setReferrer("https://www.ebay.co.uk/ulk/messages/reply?M2MContact&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=8&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=TE1798&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460&placement-type=abcd&keyword=bcd&gclid=123&mesgId=12&pid=000&ppo=ab&fdbk=no&recoId=hehe&rpo=tu&ymmmid=1&ymsid=2&yminstc=a&adcamp_locationsrc=abc&pu=12345&loc=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460");
    event.setReferrer("https://www.ebay.co.uk/ulk/usr/amyonline2010?mkevt=1&mkcid=8");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=11923&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=abc&placement-type=abcd&keyword=bcd&gclid=123");
    event.setReferrer("https://www.ebay.co.uk/ulk/messages/reply?M2MContact&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=8&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=TE1798&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460&placement-type=abcd&keyword=bcd&gclid=123&mesgId=12&pid=000&ppo=ab&fdbk=no&recoId=hehe&rpo=tu&ymmmid=1&ymsid=2&yminstc=a&adcamp_locationsrc=abc&pu=12345&loc=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460");
    event.setReferrer("https://www.ebay.co.uk/ulk/usr/amyonline2010?mkevt=1&mkcid=8");
    response = postMcsResponse(eventsPath, endUserCtxAndroidNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7");
    event.setReferrer("https://www.ebay.co.uk/ulk/messages/reply?M2MContact&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=8&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=TE1798&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460&placement-type=abcd&keyword=bcd&gclid=123&mesgId=12&pid=000&ppo=ab&fdbk=no&recoId=hehe&rpo=tu&ymmmid=1&ymsid=2&yminstc=a&adcamp_locationsrc=abc&pu=12345&loc=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460");
    event.setReferrer("https://www.ebay.co.uk/ulk/usr/amyonline2010?mkevt=1&mkcid=8");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7");
    event.setReferrer("https://www.ebay.co.uk/ulk/messages/reply?M2MContact&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=8&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=TE1798&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460&placement-type=abcd&keyword=bcd&gclid=123&mesgId=12&pid=000&ppo=ab&fdbk=no&recoId=hehe&rpo=tu&ymmmid=1&ymsid=2&yminstc=a&adcamp_locationsrc=abc&pu=12345&loc=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460");
    event.setReferrer("https://www.ebay.co.uk/ulk/usr/amyonline2010?mkevt=1&mkcid=8");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=11923&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=abc&placement-type=abcd&keyword=bcd&gclid=123");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("ebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=8&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=TE1798&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460&placement-type=abcd&keyword=bcd&gclid=123&mesgId=12&pid=000&ppo=ab&fdbk=no&recoId=hehe&rpo=tu&ymmmid=1&ymsid=2&yminstc=a&adcamp_locationsrc=abc&pu=12345&loc=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=11923&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=abc&placement-type=abcd&keyword=bcd&gclid=123");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());

    event.setTargetUrl("padebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=8&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=TE1798&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&choco_bs=1&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460&placement-type=abcd&keyword=bcd&gclid=123&mesgId=12&pid=000&ppo=ab&fdbk=no&recoId=hehe&rpo=tu&ymmmid=1&ymsid=2&yminstc=a&adcamp_locationsrc=abc&pu=12345&loc=https%3A%2F%2Fwww.ebay.co.uk%2Fi%2F393515271460");
    event.setReferrer("");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, event);
    assertEquals(201, response.getStatus());


    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerEpn = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesEpn = pollFromKafkaTopic(
            consumerEpn, Arrays.asList("dev_listened-epn"), 20, 30 * 1000);
    consumerEpn.close();

    Map<Long, ListenerMessage> listenerMessagesEpnExcludeRover = listenerMessageExcludeRover(listenerMessagesEpn);
    assertEquals(14, listenerMessagesEpnExcludeRover.size());


    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerSocialMedia = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesSocialMedia = pollFromKafkaTopic(
            consumerSocialMedia, Arrays.asList("dev_listened-social-media"), 2, 30 * 1000);
    consumerSocialMedia.close();
    assertEquals(1, listenerMessagesSocialMedia.size());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerDisplay = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesDisplay = pollFromKafkaTopic(
            consumerDisplay, Arrays.asList("dev_listened-display"), 10, 30 * 1000);
    consumerDisplay.close();
    assertEquals(8, listenerMessagesDisplay.size());
  }

  @Test
  public void testSyncResource() throws InterruptedException {
    Event event = new Event();
    event.setReferrer("https://www.ebay.com");
    event.setTargetUrl("https://www.ebayadservices.com/marketingtracking/v1/sync?guid=abcd&adguid=defg");

    // success request
    Response response = postMcsResponse(syncPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-ENDUSERCTX
    response = postMcsResponse(syncPath, null, tracking, event);
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-TRACKING
    response = postMcsResponse(syncPath, endUserCtxiPhone, null, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage  = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());
  }

  @Test
  public void testEncodedRotationResource() throws IOException, InterruptedException {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("http://www.ebay.com/sch/i.html?gclid=Cj0KCQjwj_XpBRCCARIsAItJiuTTQHX51hBamlNebEm1QEHuWzeb3mCTmjEqHjjg68fmARPt8jGbSyAaAmEuEALw_wcB&_recordsize=12&crlp=307364301297_&campid=1615587334&_sastarttime=1591534514&_saved=1&rlsatarget=dsa%252D19959388920&loc=9011814&mkrid=711%252D153677%252D346401%252D4&mkevt=1&adpos=1o3&_nkw=giannini%20awn%20guitar&device=m&mkcid=2");

    // encoded rotationId twice
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // encoded rotationId
    event.setTargetUrl("http://www.ebay.com/sch/i.html?gclid=CjwKCAjwx_boBRA9EiwA4kIELjFk97MMWjBrtG3pAqjES2Q945DOFkXdwC6PnxKRjJPJ6N15B5yIyhoC30sQAvD_BwE&_recordsize=12&geo_id=10232&MT_ID=69&campid=495209116&crlp=350660010882_&_sastarttime=1591553855&_saved=1&rlsatarget=kwd%252D366275174317&keyword=women%2520femme%2520perfume&abcId=473846&loc=1012226&mkrid=711%2D42618%2D2056%2D0&mkevt=1&adpos=1o2&_nkw=boudoir%20secrets%20perfume&device=m&mkcid=2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // normal rotationId
    event.setTargetUrl("http://www.ebay.co.uk/sch/i.html?gclid=EAIaIQobChMI78%252DP1Z2s6AIVy7TtCh11YAAxEAMYAyAAEgJYavD_BwE&recordsize=12&geo_id=32251&MT_ID=584396&campid=1537903894&crlp=349241947881&_sastarttime=1591532529&_saved=1&rlsatarget=aud%252D629407025665%253Akwd%252D312440981206&keyword=stonemarket%2520paving%2520slabs&abcId=1139886&loc=1006563&mkrid=710-154084-933926-0&mkevt=1&_nkw=stonemarket+paving+slabs&device=t&mkcid=2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
    assertEquals(201, response.getStatus());

    // invalid rotationId
    event.setTargetUrl("http://www.ebay.co.uk/sch/i.html?gclid=EAIaIQobChMI78%252DP1Z2s6AIVy7TtCh11YAAxEAMYAyAAEgJYavD_BwE&recordsize=12&geo_id=32251&MT_ID=584396&campid=1537903895&crlp=349241947881&_sastarttime=1591532529&_saved=1&rlsatarget=aud%252D629407025665%253Akwd%252D312440981206&keyword=stonemarket%2520paving%2520slabs&abcId=1139886&loc=1006563&mkrid=711%2D42618%2D2056%0&mkevt=1&_nkw=stonemarket+paving+slabs&device=t&mkcid=2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, event);
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
    Map<Long, Long> campaignRotationMap = new HashMap<>();
    for (ListenerMessage listenerMessage: listenerMessagesPaidSearch.values()) {
      campaignRotationMap.put(listenerMessage.getCampaignId(), listenerMessage.getDstRotationId());
    }

    assertEquals("7111536773464014", campaignRotationMap.get(1615587334L).toString());
    assertEquals("7114261820560", campaignRotationMap.get(495209116L).toString());
    assertEquals("7101540849339260", campaignRotationMap.get(1537903894L).toString());
    assertEquals("-1", campaignRotationMap.get(1537903895L).toString());
  }

  @Test
  public void testCheckoutAPIClickAndRoiEventsResource() throws InterruptedException {
    // Test Checkout api click
    Event event = new Event();
    event.setReferrer("");
    event.setTargetUrl("http://www.ebay.com/itm/184157407508?mkevt=1&mkcid=1");
    EventPayload eventPayload = new EventPayload();
    eventPayload.setCheckoutAPIClickTs("1604566345000");
    event.setPayload(eventPayload);

    Response response = postMcsResponse(eventsPath, endUserCtxCheckoutAPI, tracking, event);
    assertEquals(201, response.getStatus());

    // Test Checkout api ROI
    ROIEvent roiEvent = new ROIEvent();
    roiEvent.setItemId("192658398245");
    roiEvent.setTransType("BIN-FP");
    roiEvent.setUniqueTransactionId("1677235978009");
    roiEvent.setTransactionTimestamp("1504566344000");

    Map<String, String> payload = new HashMap<String, String>();
    payload.put("roisrc", "2");
    payload.put("api", "1");
    payload.put("BIN-FP", "1");
    payload.put("siteId", "0");
    roiEvent.setPayload(payload);

    Response response1 = client.target(svcEndPoint).path(roiPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxCheckoutAPI)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(roiEvent));
    assertEquals(201, response1.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerROI = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesROI = pollFromKafkaTopic(
            consumerROI, Arrays.asList("dev_listened-roi"), 1, 30 * 1000);
    consumerROI.close();

    Map<Long, ListenerMessage> listenerMessagesROIExcludeRover = listenerMessageExcludeRover(listenerMessagesROI);
    assertEquals(1, listenerMessagesROIExcludeRover.size());
  }

  @Test
  public void testTrackResource() {
    UnifiedTrackingEvent event = new UnifiedTrackingEvent();
    event.setProducerEventId("123");
    event.setProducerEventTs(System.currentTimeMillis());
    Map<String, String> payload = new HashMap<>();
    event.setPayload(payload);
    Response response = postMcsResponse(trackPath, event);
    assertEquals(201, response.getStatus());
  }

  @Test
  public void testClickResourceFromEPNPromotedListings() throws InterruptedException {
    String marketingStatusCode = "200";
    Event event = new Event();
    event.setReferrer("https://www.ebay.co.uk/gum/v1/stick?q=carpet%20cleaner");
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet");

    // click from promoted listing iframe on ebay partner site, pass
    Response response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click from non-epn channel
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=2&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click with no mksrc parameter
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click with mksrc<>"PromotedListings"
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=Promoted&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click with no plrfr
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click with plrfr=""
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click with original referer="", pass
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet");
    event.setReferrer("");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxNoReferer, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click with original referer which is not ebay domain, pass
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet");
    event.setReferrer("https://www.gumtree.com/v1/stick?q=carpet");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // click with original referer which is ebay domain but doesn't contain '%/gum/%', the referer will be internal
    event.setTargetUrl("https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet");
    event.setReferrer("https://www.ebay.com/itm/1234567");
    response = postMcsResponseWithStatusCode(eventsPath, endUserCtxMweb, tracking, event, marketingStatusCode);
    assertEquals(201, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerEpn = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesEpn = pollFromKafkaTopic(
            consumerEpn, Arrays.asList("dev_listened-epn"), 8, 30 * 1000);
    consumerEpn.close();
    // only the no referer one and the valid one with correct plrfr can pass
    Map<Long, ListenerMessage> listenerMessagesEpnExcludeRover = listenerMessageExcludeRover(listenerMessagesEpn);
    assertEquals(3, listenerMessagesEpnExcludeRover.size());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
            consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 2, 30 * 1000);
    consumerPaidSearch.close();
    assertEquals(0, listenerMessagesPaidSearch.size());
  }

  @Test
  public void testProcessPreInstallROIEvent() throws Exception {
    String token = tokenGenerator.getToken().getAccessToken();
    // Test event cases
    ROIEvent roiEventPreInstall = constructROIEvent();
    Map<String, String> roiPayloadMap = new HashMap<String, String>();
    roiPayloadMap.put("siteId", "77");
    roiPayloadMap.put("roisrc", "5");
    roiPayloadMap.put("usecase", "prm");
    roiPayloadMap.put("mppid", "92");
    roiPayloadMap.put("rlutype", "1");
    roiPayloadMap.put("mrollp", "79");

    roiEventPreInstall.setPayload(roiPayloadMap);

    Response response1 = client.target(svcEndPoint).path(roiPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxAndroid)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(roiEventPreInstall));
    assertEquals(201, response1.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerDisplay = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesDisplay = pollFromKafkaTopic(
            consumerDisplay, Arrays.asList("dev_listened-display"), 1, 30 * 1000);
    consumerDisplay.close();
    // dummy click will be dropped into display click topic
    assertEquals(1, listenerMessagesDisplay.size());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerROI = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesROI = pollFromKafkaTopic(
            consumerROI, Arrays.asList("dev_listened-roi"), 1, 30 * 1000);
    consumerROI.close();
    Map<Long, ListenerMessage> listenerMessagesROIExcludeRover = listenerMessageExcludeRover(listenerMessagesROI);
    assertEquals(1, listenerMessagesROIExcludeRover.size());
  }

  @Test
  public void testPlaceOfferAPIClickAndRoiEventsResource() throws InterruptedException {
    // Test Checkout api click
    Event event = new Event();
    event.setReferrer("");
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&campid=1234567899&customid=234&nrd=1&api=1&toolid=10006");
    EventPayload eventPayload = new EventPayload();
    eventPayload.setPlaceOfferAPIClickTs("1604566345000");
    event.setPayload(eventPayload);

    Response response = postMcsResponse(eventsPath, endUserCtxPlaceOfferAPI, tracking, event);
    assertEquals(201, response.getStatus());

    // Test Checkout api ROI
    ROIEvent roiEvent = new ROIEvent();
    roiEvent.setItemId("192658398245");
    roiEvent.setTransType("BIN-FP");
    roiEvent.setUniqueTransactionId("1677235978009");
    roiEvent.setTransactionTimestamp("1504566344000");

    Map<String, String> payload = new HashMap<String, String>();
    payload.put("roisrc", "6");
    payload.put("api", "1");
    payload.put("BIN-FP", "1");
    payload.put("siteId", "0");
    roiEvent.setPayload(payload);

    Response response1 = client.target(svcEndPoint).path(roiPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxPlaceOfferAPI)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(roiEvent));
    assertEquals(201, response1.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerEpn = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesEpn = pollFromKafkaTopic(
            consumerEpn, Arrays.asList("dev_listened-epn"), 2, 30 * 1000);
    consumerEpn.close();

    Map<Long, ListenerMessage> listenerMessagesEpnExcludeRover = listenerMessageExcludeRover(listenerMessagesEpn);
    assertEquals(1, listenerMessagesEpnExcludeRover.size());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerROI = kafkaCluster.createConsumer(
            LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesROI = pollFromKafkaTopic(
            consumerROI, Arrays.asList("dev_listened-roi"), 1, 30 * 1000);
    consumerROI.close();

    Map<Long, ListenerMessage> listenerMessagesROIExcludeRover = listenerMessageExcludeRover(listenerMessagesROI);
    assertEquals(1, listenerMessagesROIExcludeRover.size());
  }

  /**
   * Load properties
   * @param fileName
   */
  public Properties getProperties(String fileName) throws Exception {
    File resourcesDirectory = new File("src/test/resources/META-INF/configuration/Dev/config");
    String resourcePath = resourcesDirectory.getAbsolutePath() + "/";
    String propertiesFile = resourcePath + fileName;
    Properties properties = new Properties();
    properties.load(new FileReader(propertiesFile));
    return properties;
  }

  public ROIEvent constructROIEvent() {
    ROIEvent roiEvent = new ROIEvent();
    roiEvent.setItemId("192658398245");
    roiEvent.setTransType("BIN-MobileApp");
    roiEvent.setUniqueTransactionId("1225203159023");
    roiEvent.setTransactionTimestamp("1620385327000");
    return roiEvent;
  }

  public Map<Long, ListenerMessage> listenerMessageExcludeRover(Map<Long, ListenerMessage> listenerMessageMap) {
    Map<Long, ListenerMessage> listenerMessageMapExcludeRover = new HashMap<>();

    for (Map.Entry<Long, ListenerMessage> entry : listenerMessageMap.entrySet()) {
      ListenerMessage listenerMessage = entry.getValue();

      Matcher roverSitesMatcher = roversites.matcher(listenerMessage.getUri().toLowerCase());
      if (!roverSitesMatcher.find()) {
        listenerMessageMapExcludeRover.put(entry.getKey(), entry.getValue());
      }
    }

    return listenerMessageMapExcludeRover;
  }

  @Test
  public void testNonSojTag() {
    Event event = new Event();
    event.setReferrer("");
    event.setTargetUrl("https://www.qa.ebay.com/?mkevt=1&mkcid=8&mkpid=12&bu=43551630917&emsid=e11051.m44.l1139&crd=20190801034425&segname=AD379737195_GBH_BBDBENNEWROW_20180813_ZK&ymmmid=1740915&ymsid=1495596781385&yminstc=8878");
    EventPayload eventPayload = new EventPayload();
    eventPayload.setPlaceOfferAPIClickTs("1604566345000");
    event.setPayload(eventPayload);

    Response response = postMcsResponse(eventsPath, endUserCtxPlaceOfferAPI, tracking, event);
    assertEquals(201, response.getStatus());

  }

  @Test
  public void testGCXEmailClick() {
    Event event = new Event();
    event.setReferrer("");
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=29&mkrid=711-53200-19255-0&campid=1234567899&customid=234&nrd=1&api=1&toolid=10006");
    EventPayload eventPayload = new EventPayload();
    eventPayload.setPlaceOfferAPIClickTs("1641970800491");
    event.setPayload(eventPayload);
    Response response = postMcsResponse(eventsPath, endUserCtxPlaceOfferAPI, tracking, event);
    assertEquals(HttpStatus.CREATED.value(), response.getStatus());
  }

  @Test
  public void testGCXMessageCentreClick() {
    Event event = new Event();
    event.setReferrer("");
    event.setTargetUrl("https://www.ebay.com/?mkevt=1&mkcid=30&mkrid=711-53200-19255-0&campid=1234567899&customid=234&nrd=1&api=1&toolid=10006");
    EventPayload eventPayload = new EventPayload();
    eventPayload.setPlaceOfferAPIClickTs("1641970896430");
    event.setPayload(eventPayload);
    Response response = postMcsResponse(eventsPath, endUserCtxPlaceOfferAPI, tracking, event);
    assertEquals(HttpStatus.CREATED.value(), response.getStatus());
  }
  
  @Test
  public void testThirdPartyClick() {
    Event event = new Event();
    event.setReferrer("");
    event.setTargetUrl("https://ebay.live/fr/upcoming-events/181?mkevt=1&mkcid=7");
    Response response = postMcsResponse(eventsPath, endUserCtxPlaceOfferAPI, tracking, event);
    assertEquals(HttpStatus.CREATED.value(), response.getStatus());
  }
}
