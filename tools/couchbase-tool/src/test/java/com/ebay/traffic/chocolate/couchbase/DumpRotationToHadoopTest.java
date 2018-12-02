package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.gson.Gson;
import org.junit.*;
import org.mockito.Mockito;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class DumpRotationToHadoopTest {
  private static final String VIEW_NAME = "last_update_time";
  private static final String DESIGNED_DOC_NAME = "default";
  private static final String TEMP_FILE_FOLDER = "/tmp/rotation2/";
  private static final String TEMP_FILE_PREFIX = "rotation_test_";
  private static CorpRotationCouchbaseClient couchbaseClient;
  private static Bucket bucket;
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
  public static void tearDown() {
    CouchbaseClientMock.tearDown();
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
    rotationInfo.setLast_update_time(1540143551289l);
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
  public void testDumpHourlyFileFromCouchbase() throws IOException {
    // set initialed cb related info
    DumpRotationToHadoop.setBucket(bucket);
    Properties couchbasePros = new Properties();
    couchbasePros.put("couchbase.corp.rotation.designName", DESIGNED_DOC_NAME);
    couchbasePros.put("couchbase.corp.rotation.viewName", VIEW_NAME);
    couchbasePros.put("chocolate.elasticsearch.url", "http://10.148.181.34:9200");
    ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
    DumpRotationToHadoop.setCouchbasePros(couchbasePros);

    // run dump job
    createFolder();
    DumpRotationToHadoop.dumpFileFromCouchbase("1287554384000", "1603173584000", TEMP_FILE_FOLDER + TEMP_FILE_PREFIX);

    // assertions
    File[] files = new File(TEMP_FILE_FOLDER).listFiles();
    RotationInfo rotationInfo = getTestRotationInfo();
    Gson gson = new Gson();
    boolean isFileExist = false;
    String expectedFileName = TEMP_FILE_PREFIX + "rotation-20101020135944.txt";
    for (File file : files) {
      if (file.isFile() && expectedFileName.equals(file.getName())) {
        isFileExist = true;
        FileInputStream fis = new FileInputStream(new File(TEMP_FILE_FOLDER + file.getName()));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line;
        int i = 0;
        boolean jsonEqual;
        while ((line = br.readLine()) != null) {
          i++;
          jsonEqual = JsonUtils.areEqual(gson.toJson(rotationInfo), line);
          Assert.assertTrue(jsonEqual);
        }
        Assert.assertEquals(1, i);
        br.close();

        file.deleteOnExit();
      }
    }
    Assert.assertTrue("Hourly file not generated", isFileExist);
  }

  @Test
  public void testDumpDailySnapshotFromCouchbase() throws IOException {
    // set initialed cb related info
    DumpRotationToHadoop.setBucket(bucket);
    Properties couchbasePros = new Properties();
    couchbasePros.put("couchbase.corp.rotation.designName", DESIGNED_DOC_NAME);
    couchbasePros.put("couchbase.corp.rotation.viewName", VIEW_NAME);
    couchbasePros.put("chocolate.elasticsearch.url","http://10.148.181.34:9200");
    ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
    DumpRotationToHadoop.setCouchbasePros(couchbasePros);

    // run dump job
    createFolder();
    DumpRotationToHadoop.dumpFileFromCouchbase(null, null, TEMP_FILE_FOLDER + TEMP_FILE_PREFIX);

    // assertions
    File[] files = new File(TEMP_FILE_FOLDER).listFiles();
    RotationInfo rotationInfo = getTestRotationInfo();
    Gson gson = new Gson();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    boolean isFileExist = false;
    String expectedFileName = TEMP_FILE_PREFIX + "rotation-snapshot-" + sdf.format(new Date()) + ".txt";
    for (File file : files) {
      if (file.isFile() && expectedFileName.equals(file.getName())) {
        isFileExist = true;

        FileInputStream fis = new FileInputStream(new File(TEMP_FILE_FOLDER + file.getName()));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        int i = 0;
        boolean jsonEqual = false;
        while ((line = br.readLine()) != null) {
          i++;
          jsonEqual = JsonUtils.areEqual(gson.toJson(rotationInfo), line);
          Assert.assertTrue(jsonEqual);
        }
        Assert.assertEquals(1, i);
        br.close();

        file.deleteOnExit();
      }
    }
    Assert.assertTrue("Daily file not generated", isFileExist);
  }

  private void createFolder() {
    File directory = new File(TEMP_FILE_FOLDER);
    if (!directory.exists()) {
      directory.mkdir();
    }
  }
}
