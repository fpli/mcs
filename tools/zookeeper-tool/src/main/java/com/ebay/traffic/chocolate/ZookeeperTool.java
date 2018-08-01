package com.ebay.traffic.chocolate;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.app.raptor.chocolate.avro.PublisherCacheEntry;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import scopt.Opt;

import java.io.ByteArrayOutputStream;
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
  private static String zkRoot;

  public static void main(String args[]) throws Exception{
    buildCommandLine(args);
  }

  private static void buildCommandLine(String args[]) throws Exception{
    Options options = new Options();
    options.addOption("h", false, "help information");
    Option d = Option.builder("d").required(false).hasArg().argName("timestamp").desc("delete the znode before timestamp").build();
    Option u = Option.builder("u").required(false).build();
    options.addOption(u);
    options.addOption(d);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("h")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java -jar [OPTION]", options);
      return;
    }

    if (cmd.hasOption("d")) {
      String timestamp = cmd.getOptionValue("d");
      deleteZnodeByTimeStamp(Long.valueOf(timestamp));
    }

    if (cmd.hasOption("u")) {
      upsertZnodeIntoCouchbase();
    }
  }

  private static void deleteZnodeByTimeStamp(long timestamp) throws Exception{
    init();
    initZookeeperClient();
    deleteEntryByTimestamp(timestamp);
    closeZk();
  }

  private static void upsertZnodeIntoCouchbase() throws Exception{
    init();
    initZookeeperClient();
    connectCouchbase();
    upsertDataIntoCouchbase();
    close();
  }

  public static void init() throws Exception{
    properties = new Properties();
    InputStream in = Object.class.getResourceAsStream("/pros.properties");
    properties.load(in);
  }

  public static void initZookeeperClient() throws Exception{
    zkRoot = properties.getProperty("cache.zkrootdir");
    client = CuratorFrameworkFactory.newClient(properties.getProperty("zkconnect"), new ExponentialBackoffRetry(1000, 3));
    client.start();
    System.out.println("Curator client started");
    cacheRoot = new PathChildrenCache(client, zkRoot, true);
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

  public static void addEntry(final PublisherCacheEntry entry) throws Exception {
    Validate.notNull(client, "Client can't be null at this point");

    // Get the path we'll be making.
    String path = ZKPaths.makePath(zkRoot, Long.toString(entry.getCampaignId()));
    byte[] data = toBytes(entry);

    // Finally, set the value on Zookeeper.
    try {
      client.setData().forPath(path, data);
    } catch (KeeperException.NoNodeException e) {
      // Special scenario - if the parent isn't created, let's create it
      // automatically.
      client.create().creatingParentsIfNeeded().forPath(path, data);
    }
  }

  private static void deleteEntryByTimestamp(long timestamp) throws Exception{
    for (ChildData data : cacheRoot.getCurrentData()) {
      PublisherCacheEntry entry = fromBytes(data.getData());
      if (((timestamp- entry.getTimestamp()) / (1000  * 60 * 60 * 24)) > 3) {
        client.delete().guaranteed().forPath(zkRoot + "/" + Long.toString(entry.getCampaignId()));
        System.out.println("Successfully delete " + entry.getCampaignId());
      }
    }
    System.out.println("After clean up, there is " + cacheRoot.getCurrentData().size() + " znode");
  }


  private static byte[] toBytes(final PublisherCacheEntry entry)
      throws IOException {
    Validate.notNull(entry, "Entry can't be null");

    // Convert entry to a JSON blob.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(
        PublisherCacheEntry.SCHEMA$, stream);
    SpecificDatumWriter<PublisherCacheEntry> avroWriter = new SpecificDatumWriter<PublisherCacheEntry>(
        PublisherCacheEntry.SCHEMA$);
    avroWriter.write(entry, jsonEncoder);
    jsonEncoder.flush();
    IOUtils.closeQuietly(stream);
    return stream.toByteArray();
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

  private static void close() throws IOException{
    bucket.close();
    cluster.disconnect();
    cacheRoot.close();
    client.close();
  }

  private static void closeZk() throws IOException{
    cacheRoot.close();
    client.close();
  }
}
