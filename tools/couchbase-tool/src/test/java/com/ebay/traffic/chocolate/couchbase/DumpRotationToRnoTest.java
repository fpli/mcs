package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class DumpRotationToRnoTest {
    private static final String VIEW_NAME = "last_update_time";
    private static final String DESIGNED_DOC_NAME = "rotation_info";
    private static final String TEMP_FILE_FOLDER = "/tmp/rotation/";
    private static final String TEMP_FILE_PREFIX = "rotation_test_";
    private static CorpRotationCouchbaseClient couchbaseClient;
    private static Bucket bucket;
    private static RotationESClient rotationESClient;
    private static RestHighLevelClient restHighLevelClient;
    private Map<String, String> fileNamesMap;
    private static final DateFormat ORIGIN_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

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
        bucket.upsert(JsonDocument.create(rotationInfo.getRotation_string(), JsonObject.fromJson(new Gson().toJson(rotationInfo))));
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
        rotationInfo.setLast_update_time(1459807000000L);
        rotationInfo.setUpdate_user("yimeng");
        rotationInfo.setUpdate_date("2018-10-18 16:15:32");
        rotationInfo.setCreate_date("2018-10-18 16:15:32");
        rotationInfo.setCreate_user("chocolate");
        return rotationInfo;
    }

    @After
    public void destroy() {
        couchbaseClient.shutdown();
    }

    @Test
    public void testDumpFileFromCouchbase() throws Exception {
        // set initialed cb related info
        DumpRotationToRno.setBucket(bucket);
        Properties couchbasePros = new Properties();
        couchbasePros.put("couchbase.corp.rotation.designName", DESIGNED_DOC_NAME);
        couchbasePros.put("couchbase.corp.rotation.viewName", VIEW_NAME);
        couchbasePros.put("chocolate.elasticsearch.url","http://10.148.181.34:9200");
        DumpRotationToRno.setCouchbasePros(couchbasePros);

        // set initialed es rest high level client
        DumpRotationToRno.setEsRestHighLevelClient(restHighLevelClient);

        // run dump job
        createFolder();
        DumpRotationToRno.dumpFileFromCouchbase("1287554384000", "1603173584000", TEMP_FILE_FOLDER + TEMP_FILE_PREFIX);

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
        String esSearchStartTime = "2020-06-05 00:00:00";
        String esSearchEndTime = "2020-06-05 23:59:59";
        DumpRotationToRno.setEsRestHighLevelClient(restHighLevelClient);
        //test new create rotation count
        Integer newCreateRotationCount = DumpRotationToRno.getChangeRotationQuantity(esSearchStartTime, esSearchEndTime, RotationConstant.ES_CREATE_ROTATION_KEY);
        Assert.assertEquals("2", newCreateRotationCount.toString());

        //test update rotation count
        Integer updateRotationCount = DumpRotationToRno.getChangeRotationQuantity(esSearchStartTime, esSearchEndTime, RotationConstant.ES_UPDATE_ROTATION_KEY);
        Assert.assertEquals("1", updateRotationCount.toString());
    }

    @Test
    public void testThrowException() throws Exception {
        // set initialed cb related info
        DumpRotationToRno.setBucket(bucket);
        Properties couchbasePros = new Properties();
        couchbasePros.put("couchbase.corp.rotation.designName", DESIGNED_DOC_NAME);
        couchbasePros.put("couchbase.corp.rotation.viewName", VIEW_NAME);
        couchbasePros.put("chocolate.elasticsearch.url","http://10.148.181.34:9200");
        DumpRotationToRno.setCouchbasePros(couchbasePros);

        // set initialed es rest high level client
        DumpRotationToRno.setEsRestHighLevelClient(restHighLevelClient);

        // run dump job
        createFolder();
        DumpRotationToRno.dumpFileFromCouchbase("1591286400000", "1591372800000", TEMP_FILE_FOLDER + TEMP_FILE_PREFIX);
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
        return fnameMap;
    }

    private void checkRotationFile(String fileName, RotationInfo rotationInfo) throws IOException {
        FileInputStream fis = new FileInputStream(new File(TEMP_FILE_FOLDER + fileName));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        String line = null;
        int i = 0;
        while ((line = br.readLine()) != null) {
            i++;
            String[] fields = line.split("\\|");
            Assert.assertEquals(String.valueOf(rotationInfo.getRotation_id()), fields[0]);
            Assert.assertEquals(rotationInfo.getRotation_string(), fields[1]);
            Assert.assertEquals(rotationInfo.getRotation_name(), fields[2]);
            Assert.assertEquals(String.valueOf(rotationInfo.getChannel_id()), fields[4]);
            Assert.assertEquals(rotationInfo.getRotation_tag().get(RotationConstant.FIELD_ROTATION_CLICK_THRU_URL), fields[5]);
            Assert.assertEquals(rotationInfo.getStatus(), fields[6]);
            Assert.assertEquals("2018-05-06", fields[10]);
            Assert.assertEquals("2020-05-06", fields[11]);
            Assert.assertEquals(rotationInfo.getRotation_description(), fields[12]);
            Assert.assertEquals(String.valueOf(rotationInfo.getVendor_id()), fields[18]);
            Assert.assertEquals(rotationInfo.getVendor_name(), fields[19]);
            Assert.assertEquals(String.valueOf(MPLXClientEnum.US.getMplxClientId()), fields[22]);
            Assert.assertEquals(String.valueOf(rotationInfo.getCampaign_id()), fields[23]);
            Assert.assertEquals(MPLXClientEnum.US.getMplxClientName(), fields[24]);
            Assert.assertEquals(rotationInfo.getCampaign_name(), fields[25]);
            Assert.assertEquals("1234567890", fields[26]);
            Assert.assertEquals("2018-10-18", fields[37]);
            Assert.assertEquals("chocolate", fields[38]);
            Assert.assertEquals("yimeng", fields[39]);
        }
        Assert.assertEquals(1, i);
        br.close();
    }

    private void checkCampaignFile(String fileName, RotationInfo rotationInfo) throws IOException {
        FileInputStream fis = new FileInputStream(new File(TEMP_FILE_FOLDER + fileName));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        String line = null;
        int i = 0;
        while ((line = br.readLine()) != null) {
            i++;
            String[] fields = line.split("\\|");
            Assert.assertEquals(String.valueOf(rotationInfo.getCampaign_id()), fields[0]);
            Assert.assertEquals(rotationInfo.getCampaign_name(), fields[1]);
            Assert.assertEquals(String.valueOf(MPLXClientEnum.US.getMplxClientId()), fields[2]);
            Assert.assertEquals("2018-10-18", fields[3]);
            Assert.assertEquals(rotationInfo.getCreate_user(), fields[4]);
            Assert.assertEquals(rotationInfo.getUpdate_user(), fields[5]);
        }
        Assert.assertEquals(1, i);
        br.close();
    }
}
