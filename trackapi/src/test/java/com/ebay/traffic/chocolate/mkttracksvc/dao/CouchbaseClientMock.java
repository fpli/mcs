package com.ebay.traffic.chocolate.mkttracksvc.dao;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.JsonUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;


public class CouchbaseClientMock {
    private static final BucketConfiguration bucketConfiguration = new BucketConfiguration();
    private static CouchbaseMock couchbaseMock;
    private static Cluster cluster;
    private static Bucket bucket;
    private static int carrierPort;
    private static int httpPort;

    public static void tearDown() {
        if (cluster != null) {
            cluster.disconnect();
        }
        if (couchbaseMock != null) {
            couchbaseMock.stop();
        }
    }

    private static void createMock(@NotNull String name, @NotNull String password) throws Exception {
        bucketConfiguration.numNodes = 1;
        bucketConfiguration.numReplicas = 1;
        bucketConfiguration.numVBuckets = 1024;
        bucketConfiguration.name = name;
        bucketConfiguration.type = com.couchbase.mock.Bucket.BucketType.COUCHBASE;
        bucketConfiguration.password = password;
        ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
        configList.add(bucketConfiguration);
        couchbaseMock = new CouchbaseMock(0, configList);
        couchbaseMock.start();
        couchbaseMock.waitForStartup();
    }

    private static void getPortInfo(String bucket) throws Exception {
        httpPort = couchbaseMock.getHttpPort();
        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost("localhost").setPort(httpPort).setPath("mock/get_mcports")
                .setParameter("bucket", bucket);
        HttpGet request = new HttpGet(builder.build());
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        int status = response.getStatusLine().getStatusCode();
        if (status < 200 || status > 300) {
            throw new ClientProtocolException("Unexpected response status: " + status);
        }
        String rawBody = EntityUtils.toString(response.getEntity());
        JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, JsonObject.class);
        JsonArray portsArray = respObject.getAsJsonArray("payload");
        carrierPort = portsArray.get(0).getAsInt();
    }

    public static void createClient() throws Exception{
        createMock("default", "");
        getPortInfo("default");
        cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder()
                .bootstrapCarrierDirectPort(carrierPort)
                .bootstrapHttpDirectPort(httpPort)
                .build() ,"couchbase://127.0.0.1");
        bucket = cluster.openBucket("default", 1200, TimeUnit.SECONDS);
    }

    public static Cluster getCluster() {
        return cluster;
    }

    public static Bucket getBucket() {
        return bucket;
    }
}
