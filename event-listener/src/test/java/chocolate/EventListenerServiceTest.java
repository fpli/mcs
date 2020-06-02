package chocolate;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.constant.ErrorType;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
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
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.inject.Inject;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.generateQueryString;
import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.isLongNumeric;
import static com.ebay.traffic.chocolate.common.TestHelper.pollFromKafkaTopic;
import static org.junit.Assert.*;
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
  private String token;

  private static String eventsPath;
  private static String impressionPath;
  private static String notificationPath;
  private static String roiPath;
  private static String versionPath;
  private static String syncPath;

  private static String endUserCtxiPhone;
  private static String endUserCtxAndroid;
  private static String endUserCtxDesktop;
  private static String endUserCtxMweb;
  private static String endUserCtxNoReferer;

  private static String tracking;

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
    options.setSelfServiceKafkaProperties(kafkaCluster.getProducerProperties(
        LongSerializer.class, StringSerializer.class));
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
    notificationPath = "/marketingtracking/v1/notification";
    roiPath = "/marketingtracking/v1/roi";
    versionPath = "/marketingtracking/v1/getVersion";
    syncPath = "/marketingtracking/v1/sync";

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
      ".%20Cellular%3B1080x2094%3B2.6,deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
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

    tracking = "guid=8101a7ad1670ac3c41a87509fffc40b4,cguid=8101b2b31670ac797944836ecffb525d," +
      "tguid=8101a7ad1670ac3c41a87509fffc40b4,cobrandId=2";
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KafkaTestHelper.shutdown();
  }

  private Response postMcsResponse(String path, String endUserCtx, String tracking,
                                   MultiValueMap<String, String> parameters, Event event) {
    WebTarget webTarget = client.target(svcEndPoint).path(path);
    // add parameters
    if (MapUtils.isNotEmpty(parameters)) {
      for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
        for (String value : entry.getValue()) {
          webTarget = webTarget.queryParam(entry.getKey(), value);
        }
      }
    }

    // add headers
    Invocation.Builder builder = webTarget.request();
    if (!StringUtils.isEmpty(endUserCtx)) {
      builder = builder.header("X-EBAY-C-ENDUSERCTX", endUserCtx);
    }
    if (!StringUtils.isEmpty(tracking)) {
      builder = builder.header("X-EBAY-C-TRACKING", tracking);
    }

    return builder.header("Authorization", token).accept(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(event));
  }

  private Response postMcsResponse(String path, String endUserCtx, String tracking, int statusCode,
                                   MultiValueMap<String, String> parameters, Event event) {
    WebTarget webTarget = client.target(svcEndPoint).path(path);
    // add parameters
    if (MapUtils.isNotEmpty(parameters)) {
      for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
        for (String value : entry.getValue()) {
          webTarget = webTarget.queryParam(entry.getKey(), value);
        }
      }
    }

    // add headers
    Invocation.Builder builder = webTarget.request();
    if (!StringUtils.isEmpty(endUserCtx)) {
      builder = builder.header("X-EBAY-C-ENDUSERCTX", endUserCtx);
    }
    if (!StringUtils.isEmpty(tracking)) {
      builder = builder.header("X-EBAY-C-TRACKING", tracking);
    }

    builder.header(Constants.NODE_REDIRECTION_HEADER_NAME, statusCode);

    return builder.header("Authorization", token).accept(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(event));
  }

  @Test
  public void testEventsResource() throws InterruptedException {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=2");

    // success request
    // iphone
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // desktop
    response = postMcsResponse(eventsPath, endUserCtxDesktop, tracking, null, event);
    assertEquals(201, response.getStatus());

    // android
    response = postMcsResponse(eventsPath, endUserCtxAndroid, tracking, null, event);
    assertEquals(201, response.getStatus());

    // mweb
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, null, event);
    assertEquals(201, response.getStatus());

    // with statusCode header 200
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, 200, null, event);
    assertEquals(201, response.getStatus());

    // with statusCode header 301
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, 301, null, event);
    assertEquals(201, response.getStatus());

    // with statusCode header 404
    response = postMcsResponse(eventsPath, endUserCtxMweb, tracking, 404, null, event);
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-ENDUSERCTX
    response = postMcsResponse(eventsPath, null, tracking, null, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4001, errorMessage.getErrorCode());

    // no X-EBAY-C-TRACKING
    response = postMcsResponse(eventsPath, endUserCtxiPhone, null, null, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());

    // no referer
    event.setReferrer(null);
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, null, event);
    assertEquals(201, response.getStatus());

    // rover as referer
    event.setReferrer("https://rover.ebay.com/rover/1/709-41886-4896-0/2?mpre=https%3A%2F%2Fwww.ebay.fr%2Fulk%2Fsch%2F%3F_nkw%3Dcamescope%2520jvc%26mkevt%3D1%26mkrid%3D709-41886-4896-0%26mkcid%3D2%26keyword%3Dcamescope%2520jvc%26crlp%3D285602751787_%26MT_ID%3D58%26geo_id%3D32296%26rlsatarget%3Dkwd-119986605%26adpos%3D6o2%26device%3Dc%26loc%3D9056144%26poi%3D%26abcId%3D463856%26cmpgn%3D189547424%26sitelnk%3D&keyword=camescope%20jvc&crlp=285602751787_&MT_ID=58&geo_id=32296&rlsatarget=kwd-119986605&adpos=6o2&device=c&loc=9056144&poi=&abcId=463856&cmpgn=189547424&sitelnk=&gclid=Cj0KCQjwtMvlBRDmARIsAEoQ8zSmXHKLMq9rnAokRQtw5FQcGflfnJiPbRndTX1OvNzgImj7sGgkemsaAtw9EALw_wcB");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, null, event);
    assertEquals(201, response.getStatus());

    // rover as referer but encoded
    event.setReferrer("https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-117182-37290-0%2F2%3Fmpre%3Dhttps%253A%252F%252Fwww.ebay.com%252Fi%252F153018234148%253Fchn%253Dps%2526var%253D452828802628%26itemid%3D452828802628_153018234148%26targetid%3D477790169505%26device%3Dc%26adtype%3Dpla%26googleloc%3D9060230%26poi%3D%26campaignid%3D1746988278%26adgroupid%3D71277061587%26rlsatarget%3Dpla-477790169505%26abcId%3D1139306%26merchantid%3D6296724%26gclid%3DCj0KCQjwkoDmBRCcARIsAG3xzl8lXd3bcaLMaJ8-zY1zD-COSGJrZj-CVOht-VqgWiCtPBy_hrl38HgaAu2AEALw_wcB%26srcrot%3D711-117182-37290-0%26rvr_id%3D1973157993841%26rvr_ts%3Dc1c229cc16a0aa42c5d2b84affc9e842");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, null, event);
    assertEquals(201, response.getStatus());

    // forward rover redirect
    event.setReferrer("https://rover.ebay.com/rover/");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, null, event);
    assertEquals(201, response.getStatus());

    // forward rover fail
    event.setReferrer("https://rover.ebay.com/");
    response = postMcsResponse(eventsPath, endUserCtxNoReferer, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no query parameter
    event.setReferrer("https://www.google.com");
    event.setTargetUrl("https://www.ebay.com");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no mkevt
    event.setTargetUrl("https://www.ebay.com?mkcid=2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("https://www.ebay.com?mkcid=2&mkevt=0");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkevt=1");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("https://www.ebay.com?mkcid=99&mkevt=1");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerPaidSearch = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesPaidSearch = pollFromKafkaTopic(
      consumerPaidSearch, Arrays.asList("dev_listened-paid-search"), 8, 30 * 1000);
    consumerPaidSearch.close();

    assertEquals(8, listenerMessagesPaidSearch.size());

    // mrkt email click events
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=8&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&crd=20190801034425&segname=AD379737195_GBH_BBDBENNEWROW_20180813_ZK&ymmmid=1740915&ymsid=1495596781385&yminstc=7");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // adobe click events
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=8&mkpid=14&emsid=0&id=h1d3e4e16,2d2cb515,2d03a0a1&segname=" +
        "SOP708_SG49&country=US&pu=hrtHY5sgRPq&crd=20200211040813&sojTags=adcampid%3Did%2Cadcamppu%3Dpu%2Ccrd%3Dcrd%2C" +
        "segname%3Dsegname&adobeParams=id,p1,p2,p3,p4&adcamp_landingpage=https%3A%2F%2Fwww.ebay.de%2Fdeals" +
        "&adcamp_locationsrc=adobe");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // site email click events
    event.setTargetUrl("https://www.ebay.com?mkevt=1&mkcid=7&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // marketing SMS click events
    event.setTargetUrl("https://www.ebay.com/i/1234123132?mkevt=1&mkcid=24&smsid=111&did=222");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // site SMS click events
    event.setTargetUrl("https://www.ebay.com/i/1234123132?mkevt=1&mkcid=25&smsid=111&did=222");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());
  }

  @Test
  public void testSelfService() throws InterruptedException{
    Event event = new Event();
    event.setTargetUrl("https://www.ebay.com/i/1234123132?mkevt=1&mkcid=25&smsid=111&self_service=1&self_service_id=123");
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    collectionService.getSelfServiceProducer().flush();
    Consumer<Long, String> consumerSelfService = kafkaCluster.createConsumer(
        LongDeserializer.class, StringDeserializer.class);
    Map<Long, String> messageSelfService = pollFromKafkaTopic(
        consumerSelfService, Arrays.asList("dev_self-service"), 3, 30 * 1000);
    consumerSelfService.close();

    assertEquals(1, messageSelfService.size());
    assertTrue(messageSelfService.containsKey(123L));
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
        .post(Entity.json(event));
    assertEquals(201, response1.getStatus());
//    Response response2 = client.target(svcEndPoint).path(roiPath)
//        .request()
//        .header("X-EBAY-C-ENDUSERCTX", endUserCtx)
//        .header("X-EBAY-C-TRACKING", tracking)
//        .header("Authorization", token)
//        .accept(MediaType.APPLICATION_JSON_TYPE)
//        .post(Entity.json(event2));
//    assertEquals(201, response2.getStatus());
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
  public void testImpressionResource() throws InterruptedException {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=1");

    // success request
    // iphone
    Response response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());

    // desktop
    response = postMcsResponse(impressionPath, endUserCtxDesktop, tracking, null, event);
    assertEquals(200, response.getStatus());

    // android
    response = postMcsResponse(impressionPath, endUserCtxAndroid, tracking, null, event);
    assertEquals(200, response.getStatus());

    // mweb
    response = postMcsResponse(impressionPath, endUserCtxMweb, tracking, null, event);
    assertEquals(200, response.getStatus());

    // no X-EBAY-C-TRACKING
    response = postMcsResponse(impressionPath, endUserCtxiPhone, null, null, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());

    // no referer
    event.setReferrer(null);
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());

    // no query parameter
    event.setReferrer("https://www.google.com");
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());

    // no mkevt
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=1");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=1&mkevt=1");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4007, errorMessage.getErrorCode());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkcid=99&mkevt=2");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());

    // validate kafka message
    Thread.sleep(3000);
    KafkaSink.get().flush();
    Consumer<Long, ListenerMessage> consumerEpn = kafkaCluster.createConsumer(
      LongDeserializer.class, ListenerMessageDeserializer.class);
    Map<Long, ListenerMessage> listenerMessagesEpn = pollFromKafkaTopic(
      consumerEpn, Arrays.asList("dev_listened-epn"), 4, 30 * 1000);
    consumerEpn.close();
    assertEquals(5, listenerMessagesEpn.size());

    // mrkt email impression events
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=8&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&crd=20190801034425&segname=AD379737195_GBH_BBDBENNEWROW_20180813_ZK&ymmmid=1740915&ymsid=1495596781385&yminstc=7");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());

    // site email impression events
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=4&mkcid=7&sojTags=bu%3Dbu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    response = postMcsResponse(impressionPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testnotificationResource() {
    // notification parameters
    MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
    parameters.add("imp", "2054081");
    parameters.add("nid", "539721811729");
    parameters.add("ntype", "HOT_ITEM");
    parameters.add("evt", "WATCHITM");
    parameters.add("pnact", "1");
    parameters.add("itm", "122931100413");
    parameters.add("usrname", "91334560c9v");
    parameters.add("mc3id", "1:b463ff51-60b9-4583-850f-83c6c2658cfb:2:175684535");

    // success request
    // iphone
    Response response = postMcsResponse(notificationPath, endUserCtxiPhone, tracking, parameters, null);
    assertEquals(201, response.getStatus());

    // desktop
    response = postMcsResponse(notificationPath, endUserCtxDesktop, tracking, parameters, null);
    assertEquals(201, response.getStatus());

    // android
    response = postMcsResponse(notificationPath, endUserCtxAndroid, tracking, parameters, null);
    assertEquals(201, response.getStatus());

    // mweb
    response = postMcsResponse(notificationPath, endUserCtxMweb, tracking, parameters, null);
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-ENDUSERCTX
    response = postMcsResponse(notificationPath, null, tracking, parameters, null);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4001, errorMessage.getErrorCode());

    // no X-EBAY-C-TRACKING
    response = postMcsResponse(notificationPath, endUserCtxiPhone, null, parameters, null);
    assertEquals(200, response.getStatus());
    errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());

    // no query parameter
    response = postMcsResponse(notificationPath, endUserCtxiPhone, tracking, null, null);
    assertEquals(201, response.getStatus());
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
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // success request no cguid
    // iphone
    response = postMcsResponse(eventsPath, endUserCtxiPhone, trackingNoCguid, null, event);
    assertEquals(201, response.getStatus());
  }

  @Test
  public void testDeeplinkResource() {
    Event event = new Event();
    event.setReferrer("www.google.com");
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");

    // success request
    // iphone
    Response response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    //no target url in deeplink case
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage = response.readEntity(ErrorType.class);
    assertEquals(4009, errorMessage.getErrorCode());

    //invalid target url in deeplink case
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=http%3A%2F%2Frover.ebay.com%2Frover%2F1%2F710-53481-19255-0%2F1%3Fff3%3D2");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no query parameter
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2F");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no mkevt
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // invalid mkevt
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D0%26mkrid%3D724-218635-24755-0%26mkcid%3D16%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // invalid mkcid
    // service will pass but no message to kafka
    event.setTargetUrl("ebay://link/?nav=item.view&id=143421740982&referrer=https%3A%2F%2Fwww.ebay.it%2Fi%2F143421740982%3Fitemid%3D143421740982%26prid%3D143421740982%26norover%3D1%26siteid%3D101%26mkevt%3D1%26mkrid%3D724-218635-24755-0%26mkcid%3D99%26adsetid%3D23843848068040175%26adid%3D23843848069230175%26audtag%3DMID_R02%26tag4%3D23843848068040175");
    response = postMcsResponse(eventsPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());
  }

  @Test
  public void testSyncResource() throws InterruptedException {
    Event event = new Event();
    event.setReferrer("https://www.ebay.com");
    event.setTargetUrl("https://www.ebayadservices.com/marketingtracking/v1/sync?guid=abcd&adguid=defg");

    // success request
    Response response = postMcsResponse(syncPath, endUserCtxiPhone, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-ENDUSERCTX
    response = postMcsResponse(syncPath, null, tracking, null, event);
    assertEquals(201, response.getStatus());

    // no X-EBAY-C-TRACKING
    response = postMcsResponse(syncPath, endUserCtxiPhone, null, null, event);
    assertEquals(200, response.getStatus());
    ErrorType errorMessage  = response.readEntity(ErrorType.class);
    assertEquals(4002, errorMessage.getErrorCode());
  }
}
