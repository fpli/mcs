//package com.ebay.traffic.chocolate.mkttracksvc;
//
//import org.junit.runner.RunWith;
//import org.springframework.boot.context.embedded.LocalServerPort;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
//import org.springframework.test.context.junit4.SpringRunner;
//
//@RunWith(SpringRunner.class)
//@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
//    properties = {"GingerClient.testService.testClient.endpointUri=http://localhost",
//        "GingerClient.testService.testClient.readTimeout=5000"})
//public class MkttracksvcApplicationTest {
//  @LocalServerPort
//  private int port;
////
////  @Inject
////  private ISecureTokenManager tokenGenerator;
//
////  @Test
////  public void serviceTest() throws TokenCreationException {
////    /*String token = tokenGenerator.getToken().getAccessToken();
////    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
////    Client client = ClientBuilder.newClient(configuration);
////    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
////    endpoint = endpoint + ":" + port;
////    String result = client.target(endpoint).path("/tracksvc/v1/snid/hello").request()
////        .header("Authorization", token).get(String.class);
////    assertEquals("Hello from Raptor IO", result);*/
////  }
//}
