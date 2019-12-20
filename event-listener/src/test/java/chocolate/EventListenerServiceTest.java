package chocolate;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.eventlistener.constant.ErrorType;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.ebay.traffic.chocolate.common.TestHelper.pollFromKafkaTopic;
import static org.junit.Assert.assertEquals;

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

  @LocalServerPort
  private int port;

  @Inject
  private ISecureTokenManager tokenGenerator;

  private boolean initialized = false;

  private Client client;
  private String svcEndPoint;

  private final String eventsPath = "/marketingtracking/v1/events";
  private final String impressionPath = "/marketingtracking/v1/impression";
  private final String versionPath = "/marketingtracking/v1/getVersion";

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
    String token = tokenGenerator.getToken().getAccessToken();

    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=2");

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
      ".com%2Fmkttestappweb%2Fi%2FApple-iPhone-8-Plus-256gb-Gold%2F290016063137%3Fmkevt%3D1%26mkcid%3D2," +
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
    Response response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // desktop
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxDesktop)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // android
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxAndroid)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // mweb
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxMweb)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-ENDUSERCTX
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4001, errorMessage.getErrorCode());

    // no X-EBAY-C-TRACKING
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());

    // no referer
    event.setReferrer(null);
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    // TODO: return 201 for now
    assertEquals(201, response.getStatus());
//    assertEquals(200, response.getStatus());
//    errorMessageV3 = response.readEntity(ErrorMessageV3.class);
//    assertEquals(4003, errorMessageV3.getErrors().get(0).getErrorId());

    // rover as referer
    event.setReferrer("https://rover.ebay.com/rover/1/709-41886-4896-0/2?mpre=https%3A%2F%2Fwww.ebay.fr%2Fulk%2Fsch%2F%3F_nkw%3Dcamescope%2520jvc%26mkevt%3D1%26mkrid%3D709-41886-4896-0%26mkcid%3D2%26keyword%3Dcamescope%2520jvc%26crlp%3D285602751787_%26MT_ID%3D58%26geo_id%3D32296%26rlsatarget%3Dkwd-119986605%26adpos%3D6o2%26device%3Dc%26loc%3D9056144%26poi%3D%26abcId%3D463856%26cmpgn%3D189547424%26sitelnk%3D&keyword=camescope%20jvc&crlp=285602751787_&MT_ID=58&geo_id=32296&rlsatarget=kwd-119986605&adpos=6o2&device=c&loc=9056144&poi=&abcId=463856&cmpgn=189547424&sitelnk=&gclid=Cj0KCQjwtMvlBRDmARIsAEoQ8zSmXHKLMq9rnAokRQtw5FQcGflfnJiPbRndTX1OvNzgImj7sGgkemsaAtw9EALw_wcB");
    response = client.target(svcEndPoint).path(eventsPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // rover as referer but encoded
    event.setReferrer("https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-117182-37290-0%2F2%3Fmpre%3Dhttps%253A%252F%252Fwww.ebay.com%252Fi%252F153018234148%253Fchn%253Dps%2526var%253D452828802628%26itemid%3D452828802628_153018234148%26targetid%3D477790169505%26device%3Dc%26adtype%3Dpla%26googleloc%3D9060230%26poi%3D%26campaignid%3D1746988278%26adgroupid%3D71277061587%26rlsatarget%3Dpla-477790169505%26abcId%3D1139306%26merchantid%3D6296724%26gclid%3DCj0KCQjwkoDmBRCcARIsAG3xzl8lXd3bcaLMaJ8-zY1zD-COSGJrZj-CVOht-VqgWiCtPBy_hrl38HgaAu2AEALw_wcB%26srcrot%3D711-117182-37290-0%26rvr_id%3D1973157993841%26rvr_ts%3Dc1c229cc16a0aa42c5d2b84affc9e842");
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // forward rover redirect
    event.setReferrer("https://rover.ebay.com/rover/");
    response = client.target(svcEndPoint).path(eventsPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // forward rover fail
    event.setReferrer("https://rover.ebay.com/");
    response = client.target(svcEndPoint).path(eventsPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // no query parameter
    event.setReferrer("https://www.google.com");
    event.setTargetUrl("https://www.ebay.com");
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // no mkevt
    event.setTargetUrl("https://www.ebay.com?mkcid=2");
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("https://www.ebay.com?mkcid=2&mkevt=0");
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkevt=1");
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkcid=99&mkevt=1");
    response = client.target(svcEndPoint).path(eventsPath)
      .request()
      .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
      .header("X-EBAY-C-TRACKING", tracking)
      .header("Authorization", token)
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

    assertEquals(5, listenerMessagesPaidSearch.size());

    // mrkt email click events
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=8&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&crd=20190801034425&segname=AD379737195_GBH_BBDBENNEWROW_20180813_ZK&ymmmid=1740915&ymsid=1495596781385&yminstc=7");
    response = client.target(svcEndPoint).path(eventsPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(201, response.getStatus());

    // site email click events
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=7&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = client.target(svcEndPoint).path(eventsPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(201, response.getStatus());
  }

  @Test
  public void testImpressionResource() throws Exception {
    String token = tokenGenerator.getToken().getAccessToken();

    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=2");

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
            ".com%2Fmkttestappweb%2Fi%2FApple-iPhone-8-Plus-256gb-Gold%2F290016063137%3Fmkevt%3D1%26mkcid%3D2," +
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
    Response response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // desktop
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxDesktop)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // android
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxAndroid)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // mweb
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxMweb)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // no X-EBAY-C-TRACKING
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // no referer
    event.setReferrer(null);
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    // TODO: return 200 for now
    assertEquals(200, response.getStatus());
//    assertEquals(200, response.getStatus());
//    errorMessageV3 = response.readEntity(ErrorMessageV3.class);
//    assertEquals(4003, errorMessageV3.getErrors().get(0).getErrorId());

    // rover as referer but encoded
    event.setReferrer("https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-117182-37290-0%2F2%3Fmpre%3Dhttps%253A%252F%252Fwww.ebay.com%252Fi%252F153018234148%253Fchn%253Dps%2526var%253D452828802628%26itemid%3D452828802628_153018234148%26targetid%3D477790169505%26device%3Dc%26adtype%3Dpla%26googleloc%3D9060230%26poi%3D%26campaignid%3D1746988278%26adgroupid%3D71277061587%26rlsatarget%3Dpla-477790169505%26abcId%3D1139306%26merchantid%3D6296724%26gclid%3DCj0KCQjwkoDmBRCcARIsAG3xzl8lXd3bcaLMaJ8-zY1zD-COSGJrZj-CVOht-VqgWiCtPBy_hrl38HgaAu2AEALw_wcB%26srcrot%3D711-117182-37290-0%26rvr_id%3D1973157993841%26rvr_ts%3Dc1c229cc16a0aa42c5d2b84affc9e842");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxNoReferer)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // no query parameter
    event.setReferrer("https://www.google.com");
    event.setTargetUrl("https://www.ebay.com");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // no mkevt
    event.setTargetUrl("https://www.ebay.com?mkcid=2");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("https://www.ebay.com?mkcid=2&mkevt=0");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkevt=1");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkcid=99&mkevt=1");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // mrkt email impression events
    event.setTargetUrl("https://www.ebay.com?mkevt=4&mkcid=8&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&crd=20190801034425&segname=AD379737195_GBH_BBDBENNEWROW_20180813_ZK&ymmmid=1740915&ymsid=1495596781385&yminstc=7");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());

    // site email impression events
    event.setTargetUrl("https://www.ebay.com?mkevt=4&mkcid=7&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = client.target(svcEndPoint).path(impressionPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testVersion() {
    Response response = client.target(svcEndPoint).path(versionPath)
      .request()
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .get();
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testEPNResource() throws Exception {
    String token = tokenGenerator.getToken().getAccessToken();

    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=1&mksid=2345123");

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

    String tracking = "guid=8101a7ad1670ac3c41a87509fffc40b4,cguid=8101b2b31670ac797944836ecffb525d," +
            "tguid=8101a7ad1670ac3c41a87509fffc40b4,cobrandId=2";
    // success request
    // iphone
    Response response = client.target(svcEndPoint).path(eventsPath)
            .request()
            .header("X-EBAY-C-ENDUSERCTX", endUserCtxiPhone)
            .header("X-EBAY-C-TRACKING", tracking)
            .header("Authorization", token)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.json(event));
    assertEquals(201, response.getStatus());
  }
}
