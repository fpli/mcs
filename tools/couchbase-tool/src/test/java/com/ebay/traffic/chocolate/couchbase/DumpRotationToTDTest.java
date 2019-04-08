package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.*;
import org.mockito.Mockito;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class DumpRotationToTDTest {
  private static final String VIEW_NAME = "last_update_time";
  private static final String DESIGNED_DOC_NAME = "default";
  private static final String TEMP_FILE_FOLDER = "/tmp/rotation/";
  private static final String TEMP_FILE_PREFIX = "rotation_test_";
  private static CorpRotationCouchbaseClient couchbaseClient;
  private static Bucket bucket;
  private static RotationESClient rotationESClient;
  private static RestHighLevelClient restHighLevelClient;
  private Map<String, String> fileNamesMap;

  @BeforeClass
  public static void setUp() throws Exception {
    CouchbaseClientMock.createClient();

    CacheFactory cacheFactory = Mockito.mock(CacheFactory.class);
    BaseDelegatingCacheClient baseDelegatingCacheClient = Mockito.mock(BaseDelegatingCacheClient.class);
    Couchbase2CacheClient couchbase2CacheClient = Mockito.mock(Couchbase2CacheClient.class);
    when(couchbase2CacheClient.getCouchbaseClient()).thenReturn(CouchbaseClientMock.getBucket());
    when(baseDelegatingCacheClient.getCacheClient()).thenReturn(couchbase2CacheClient);
    when(cacheFactory.getClient(any())).thenReturn(baseDelegatingCacheClient);

    couchbaseClient = new CorpRotationCouchbaseClient(cacheFactory);
    bucket = CouchbaseClientMock.getBucket();

    rotationESClient = Mockito.mock(RotationESClient.class);
    restHighLevelClient = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://10.148.181.34:9200")));

    // Insert document to mocked couchbase
    RotationInfo rotationInfo = getTestRotationInfo();
    bucket.insert(JsonDocument.create(rotationInfo.getRotation_string(), JsonObject.fromJson(new Gson().toJson(rotationInfo))));
    // Create a view for mocked couchbase bucket
    DesignDocument designDoc = DesignDocument.create(
        DESIGNED_DOC_NAME,
        Arrays.asList(
            DefaultView.create(VIEW_NAME, "function (doc) { if(doc.last_update_time) {emit(doc.last_update_time, doc);}}")
        )
    );
    bucket.bucketManager().upsertDesignDocument(designDoc);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    CouchbaseClientMock.tearDown();
    rotationESClient.closeESClient(restHighLevelClient);
  }

  private static RotationInfo getTestRotationInfo() {
    RotationInfo rotationInfo = new RotationInfo();
    rotationInfo.setRotation_id(7112452905124459l);
    rotationInfo.setRotation_string("711-245290-51244-59");
    rotationInfo.setChannel_id(1);
    rotationInfo.setSite_id(1);
    rotationInfo.setCampaign_id(500000001L);
    rotationInfo.setVendor_id(2003);
    rotationInfo.setCampaign_name("testing campaignName");
    rotationInfo.setVendor_name("testing vendorName");
    rotationInfo.setRotation_name("testing RotationName");
    rotationInfo.setRotation_description("testing RotationDescription");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationTag.put(RotationConstant.FIELD_TAG_SITE_NAME, "DE");
    rotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, MPLXChannelEnum.EPN.getMplxChannelName());
    rotationTag.put(RotationConstant.FIELD_ROTATION_START_DATE, "20180506");
    rotationTag.put(RotationConstant.FIELD_ROTATION_END_DATE, "20200506");
    rotationTag.put(RotationConstant.FIELD_PLACEMENT_ID, "1234567890");
    rotationTag.put(RotationConstant.FIELD_ROTATION_CLICK_THRU_URL, "http://clickthrough.com");
    rotationInfo.setRotation_tag(rotationTag);
    long currentTime = System.currentTimeMillis();
    rotationInfo.setLast_update_time(currentTime);
    rotationInfo.setUpdate_user("yimeng");
    rotationInfo.setCreate_date("2018-10-18 16:15:32");
    rotationInfo.setCreate_user("chocolate");
    return rotationInfo;
  }

  @After
  public void destroy() {
    couchbaseClient.shutdown();
  }

  @Test
  public void testDumpFileFromCouchbase() throws IOException {
    // set initialed cb related info
    DumpRotationToTD.setBucket(bucket);
    Properties couchbasePros = new Properties();
    couchbasePros.put("couchbase.corp.rotation.designName", DESIGNED_DOC_NAME);
    couchbasePros.put("couchbase.corp.rotation.viewName", VIEW_NAME);
    couchbasePros.put("chocolate.elasticsearch.url","http://10.148.181.34:9200");
    DumpRotationToTD.setCouchbasePros(couchbasePros);

    // set initialed es rest high level client
    DumpRotationToTD.setEsRestHighLevelClient(restHighLevelClient);

    // run dump job
    createFolder();
    DumpRotationToTD.dumpFileFromCouchbase("1287554384000", "1603173584000", TEMP_FILE_FOLDER + TEMP_FILE_PREFIX);

    // assertions
    File[] files = new File(TEMP_FILE_FOLDER).listFiles();
    String fileName = null;
    Map<String, String> fnameMap = getFileNameMap();
    RotationInfo rotationInfo = getTestRotationInfo();
    for (File file : files) {
      if (file.isFile()) {
        fileName = file.getName().replaceAll(TEMP_FILE_PREFIX + "|.txt|.status", "");
        Assert.assertNotNull(fnameMap.get(fileName));
        Assert.assertEquals(fnameMap.get(fileName), file.getName());
        if (fileName.equals("rotations")) checkRotationFile(file.getName(), rotationInfo);
        if (fileName.equals("campaigns")) checkCampaignFile(file.getName(), rotationInfo);
        file.deleteOnExit();
      }
    }


  }

  @Test
  public void testGetChangeRotationCount() throws IOException {
    String esSearchStartTime = "2019-03-29 00:00:00";
    String esSearchEndTime = "2019-03-29 23:59:59";
    DumpRotationToTD.setEsRestHighLevelClient(restHighLevelClient);
    //test new create rotation count
    Integer newCreateRotationCount = DumpRotationToTD.getChangeRotationCount(esSearchStartTime, esSearchEndTime, RotationConstant.ES_CREATE_ROTATION_KEY);
    Assert.assertEquals("2", newCreateRotationCount.toString());

    //test update rotation count
    Integer updateRotationCount = DumpRotationToTD.getChangeRotationCount(esSearchStartTime, esSearchEndTime, RotationConstant.ES_UPDATE_ROTATION_KEY);
    Assert.assertEquals("3", updateRotationCount.toString());
  }

  @Test
  public void testThrowException() {
    // set initialed cb related info
    DumpRotationToTD.setBucket(bucket);
    Properties couchbasePros = new Properties();
    couchbasePros.put("couchbase.corp.rotation.designName", DESIGNED_DOC_NAME);
    couchbasePros.put("couchbase.corp.rotation.viewName", VIEW_NAME);
    couchbasePros.put("chocolate.elasticsearch.url","http://10.148.181.34:9200");
    DumpRotationToTD.setCouchbasePros(couchbasePros);

    // set initialed es rest high level client
    DumpRotationToTD.setEsRestHighLevelClient(restHighLevelClient);

    // run dump job
    createFolder();
    Boolean throwException = false;
    try {
        DumpRotationToTD.dumpFileFromCouchbase("1553734936000", "1553907736000", TEMP_FILE_FOLDER + TEMP_FILE_PREFIX);
    } catch (IOException e) {
      throwException = true;
    }
    Assert.assertEquals(true, throwException);
  }

  private void createFolder() {
    File directory = new File(TEMP_FILE_FOLDER);
    if (!directory.exists()) {
      directory.mkdir();
    }
  }

  private Map<String, String> getFileNameMap() {
    Map<String, String> fnameMap = new HashMap<String, String>();
    fnameMap.put("rotations", TEMP_FILE_PREFIX + "rotations.txt");
    fnameMap.put("campaigns", TEMP_FILE_PREFIX + "campaigns.txt");
    fnameMap.put("creatives", TEMP_FILE_PREFIX + "creatives.txt");
    fnameMap.put("rotation-creative", TEMP_FILE_PREFIX + "rotation-creative.txt");
    fnameMap.put("df", TEMP_FILE_PREFIX + "df.status");
    fnameMap.put("position_rotations", TEMP_FILE_PREFIX + "position_rotations.txt");
    fnameMap.put("position_rules", TEMP_FILE_PREFIX + "position_rules.txt");
    fnameMap.put("positions", TEMP_FILE_PREFIX + "positions.txt");
    fnameMap.put("rules", TEMP_FILE_PREFIX + "rules.txt");
    fnameMap.put("lt_roi", TEMP_FILE_PREFIX + "lt_roi.txt");
    fnameMap.put("roi_credit_v2", TEMP_FILE_PREFIX + "roi_credit_v2.txt");
    fnameMap.put("roi_v2", TEMP_FILE_PREFIX + "roi_v2.txt");
    return fnameMap;
  }

  private void checkRotationFile(String fileName, RotationInfo rotationInfo) throws IOException {
    FileInputStream fis = new FileInputStream(new File(TEMP_FILE_FOLDER + fileName));
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));

    String line = null;
    int i = 0;
    while ((line = br.readLine()) != null) {
      i++;
      if (i == 1) {
        Assert.assertEquals(RotationConstant.FILE_HEADER_ROTATIONS, line);
      } else {
        String[] fields = line.split("\\|");
        Assert.assertEquals(String.valueOf(rotationInfo.getRotation_id()), fields[0]);
        Assert.assertEquals(rotationInfo.getRotation_string(), fields[1]);
        Assert.assertEquals(rotationInfo.getRotation_name(), fields[2]);
        Assert.assertEquals(String.valueOf(rotationInfo.getChannel_id()), fields[4]);
        Assert.assertEquals(rotationInfo.getRotation_tag().get(RotationConstant.FIELD_ROTATION_CLICK_THRU_URL), fields[5]);
        Assert.assertEquals(rotationInfo.getStatus(), fields[6]);
        Assert.assertEquals(rotationInfo.getRotation_tag().get(RotationConstant.FIELD_ROTATION_START_DATE), fields[10]);
        Assert.assertEquals(rotationInfo.getRotation_tag().get(RotationConstant.FIELD_ROTATION_END_DATE), fields[11]);
        Assert.assertEquals(rotationInfo.getRotation_description(), fields[12]);
        Assert.assertEquals(String.valueOf(rotationInfo.getVendor_id()), fields[18]);
        Assert.assertEquals(rotationInfo.getVendor_name(), fields[19]);
        Assert.assertEquals(String.valueOf(MPLXClientEnum.US.getMplxClientId()), fields[22]);
        Assert.assertEquals(String.valueOf(rotationInfo.getCampaign_id()), fields[23]);
        Assert.assertEquals(MPLXClientEnum.US.getMplxClientName(), fields[24]);
        Assert.assertEquals(rotationInfo.getCampaign_name(), fields[25]);
        Assert.assertEquals("1234567890", fields[26]);
      }
    }
    Assert.assertEquals(2, i);
    br.close();
  }

  private void checkCampaignFile(String fileName, RotationInfo rotationInfo) throws IOException {
    FileInputStream fis = new FileInputStream(new File(TEMP_FILE_FOLDER + fileName));
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));

    String line = null;
    int i = 0;
    while ((line = br.readLine()) != null) {
      i++;
      if (i == 1) {
        Assert.assertEquals(RotationConstant.FILE_HEADER_CAMPAIGN, line);
      } else {
        String[] fields = line.split("\\|");
        Assert.assertEquals(String.valueOf(MPLXClientEnum.US.getMplxClientId()), fields[0]);
        Assert.assertEquals(String.valueOf(rotationInfo.getCampaign_id()), fields[1]);
        Assert.assertEquals(MPLXClientEnum.US.getMplxClientName(), fields[2]);
        Assert.assertEquals(rotationInfo.getCampaign_name(), fields[3]);
      }
    }
    Assert.assertEquals(2, i);
    br.close();
  }
}
