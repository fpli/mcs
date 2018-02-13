package com.ebay.traffic.chocolate;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.app.raptor.chocolate.avro.PublisherCacheEntry;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ZookeeperTool {
  private static CuratorFramework client = null;
  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties properties;
  private static PathChildrenCache cacheRoot;

  public static void main(String args[]) throws Exception{
    init();
    upsertDataIntoCouchbase();
    close();
  }

  public static void init() throws Exception{
    properties = new Properties();
    InputStream in = Object.class.getResourceAsStream("/pros.properties");
    properties.load(in);
    initZookeeperClient();
    connectCouchbase();
  }

  public static void initZookeeperClient() throws Exception{
    client = CuratorFrameworkFactory.newClient(properties.getProperty("zkconnect"), new ExponentialBackoffRetry(1000, 3));
    client.start();
    System.out.println("Curator client started");
    cacheRoot = new PathChildrenCache(client, properties.getProperty("cache.zkrootdir"), true);
    cacheRoot.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    System.out.println("ZK Cache started and primed with "
        + cacheRoot.getCurrentData().size() + " values");
  }

  public static List<String> listChildren(String node) {
    System.out.println("Find children of node: [{}]" + node);
    List<String> children = null;
    try {
      Stat stat = client.checkExists().forPath(node);
      if (stat != null) {
        GetChildrenBuilder childrenBuilder = client.getChildren();
        children = childrenBuilder.forPath(node);
        client.getData();
      }
    } catch (Exception e) {
      System.out.println(e);
    }
    return children;
  }

  public static void getChildData(String node)  throws Exception{
    System.out.println("Find children of node: [{}]" + node);
    for (ChildData data : cacheRoot.getCurrentData()) {
      PublisherCacheEntry entry = fromBytes(data.getData());
      System.out.println(entry);
    }

  }

  public static PublisherCacheEntry fromBytes(final byte[] bytes)
      throws IOException {
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
        PublisherCacheEntry.SCHEMA$, new String(bytes));
    SpecificDatumReader<PublisherCacheEntry> reader = new SpecificDatumReader<PublisherCacheEntry>(
        PublisherCacheEntry.SCHEMA$);
    PublisherCacheEntry deserialized = reader.read(null, decoder);
    return deserialized;
  }

  private static void connectCouchbase() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(10000).queryTimeout(5000).build();
    cluster = CouchbaseCluster.create(env, properties.getProperty("cluster"));
    cluster.authenticate(properties.getProperty("user"),
        properties.getProperty("password"));
    bucket = cluster.openBucket(properties.getProperty("bucket"),
        300, TimeUnit.SECONDS);
  }

  public static void upsertDataIntoCouchbase() throws Exception{
    for (ChildData data : cacheRoot.getCurrentData()) {
      PublisherCacheEntry entry = fromBytes(data.getData());
      if (!bucket.exists(String.valueOf(entry.getCampaignId()))) {
        bucket.upsert(StringDocument.create(String.valueOf(entry.getCampaignId()),
            String.valueOf(entry.getPublisherId())));
      }
    }
    System.out.println("Successfully upsert " + cacheRoot.getCurrentData().size() + " record");
  }

  public static void close() throws IOException{
    bucket.close();
    cluster.disconnect();
    cacheRoot.close();
    client.close();
  }
}
