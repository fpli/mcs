package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.avro.PublisherCacheEntry;
import com.ebay.app.raptor.chocolate.common.Hostname;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Curator Zookeeper client wrapper
 *
 * @author huiclu
 */
public class FilterZookeeperClient {

    /** logging instance */
    private static final Logger logger = Logger.getLogger(FilterZookeeperClient.class);
    /**Singleton instance*/
    private static volatile FilterZookeeperClient INSTANCE = null;
    /** Curator client */
    private volatile CuratorFramework client = null;
    /** Children cache client */
    private volatile PathChildrenCache zkCache = null;
    /** Driver ID path client */
    private volatile PersistentEphemeralNode driverIdNode = null;
    /** Function for NODE ADD events */
    private Consumer<PublisherCacheEntry> addEntryCallback = null;
    /** Zookeeper path root directory */
    private final String zkRootDir;
    /** Zookeeper String */
    private final String zkConnStr;
    /** The driver ID node path */
    private final String zkDriverIdNodePath;

    /**Constructor for FilterZookeeperClient*/
    private FilterZookeeperClient(String zkConnStr, String zkRootDir, String zkDriverIdNodePath) {
        logger.info("Initialized cache with zkRootDir=" + zkRootDir + " conn str=" + zkConnStr +
                " zkDriverIdNodePath=" + zkDriverIdNodePath);
        Validate.isTrue(StringUtils.isNotBlank(zkConnStr), "ZK conn str can't be blank");
        Validate.isTrue(StringUtils.isNotBlank(zkRootDir), "ZK root dir can't be blank");
        Validate.isTrue(StringUtils.isNotBlank(zkDriverIdNodePath), "ZK driver ID path can't be blank");
        this.zkConnStr = zkConnStr;
        this.zkRootDir = zkRootDir;
        this.zkDriverIdNodePath = zkDriverIdNodePath;
    }

    /**Singleton */
    public static FilterZookeeperClient getInstance() {
        if (INSTANCE == null) {
            synchronized (FilterZookeeperClient.class) {
                if (INSTANCE == null)
                    init(ApplicationOptions.getInstance());
            }
        }
        return INSTANCE;
    }

    /** Initialize the instance with a default callback. */
    public static synchronized void init(ApplicationOptions options) {
        final Consumer<PublisherCacheEntry> callback = t -> {
            logger.debug("Got new entry from Zookeeper=" + t.toString());
            CouchbaseClient.getInstance().addMappingRecord(t.getCampaignId(), t.getPublisherId());
            CampaignPublisherMappingCache.getInstance().addMapping(t.getCampaignId(), t.getPublisherId());
        };
        init(callback, options.getPublisherCacheZkConnectString(), options.getPublisherCacheZkRoot(),
                options.getZkDriverIdNode());
    }

    /** Initialize the instance with a callback. */
    private static synchronized void init(final Consumer<PublisherCacheEntry> callback,
                                  String zkConnStr, String zkRootDir, String zkDriverIdNodePath) {
        Validate.isTrue(INSTANCE == null || INSTANCE.client == null,
                "Instance must be null or closed at this point");
        INSTANCE = new FilterZookeeperClient(zkConnStr, zkRootDir, zkDriverIdNodePath);
        try {
            INSTANCE.start(callback);
        } catch (Exception e) {
            logger.fatal("Failed to start publisher cache zookeeper client", e);
            throw new RuntimeException(e);
        }
    }



    /**
     * Initialize.
     *
     * @param callback
     *            To be invoked when we have a node addition.
     * @throws Exception
     *             when an error occurs during startup
     */
    public synchronized void start(final Consumer<PublisherCacheEntry> callback) throws Exception{
        Validate.notNull(callback, "Callback must not be null");
        Validate.isTrue(client == null && zkCache == null,
                "Client must be null at this point");
        this.addEntryCallback = callback;

        // Start up the Zookeeper instances.
        client = CuratorFrameworkFactory.newClient(zkConnStr,
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        logger.info("Curator client started.");

        // Attempt to create the "persistent ephemeral" node.
        driverIdNode = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL,
                zkDriverIdNodePath, Hostname.HOSTNAME.getBytes());
        logger.info("Creating driver ID node " + this.zkDriverIdNodePath + " with value of " + Hostname.HOSTNAME);
        driverIdNode.start();

        boolean nodeCreated = driverIdNode.waitForInitialCreate(25, TimeUnit.SECONDS);
        if (nodeCreated) {
            logger.info("Driver ID node successfully created.");
        } else {
            logger.error("Could not create driver ID node " + this.zkDriverIdNodePath + "; driver ID already in use?");
            throw new Exception("Could not create driver ID node " + this.zkDriverIdNodePath);
        }

        // Now create the cache itself.
        zkCache = new PathChildrenCache(client, this.zkRootDir, true);
        zkCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        logger.info("ZK Cache started and primed with "
                + zkCache.getCurrentData().size() + " values");

        // Drop in the callback for listening.
        zkCache.getListenable().addListener(
                (client, event) ->{
                    // the only event we are interested in is CHILD_ADDED.
                    if (event.getType() != PathChildrenCacheEvent.Type.CHILD_ADDED)
                        return;
                    ChildData data = event.getData();
                    try {
                        PublisherCacheEntry entry = fromBytes(data.getData());
                        addEntryCallback.accept(entry);
                    } catch (IOException e) {
                        logger.warn("Unable to serialize entry for path="
                                + data.getPath());
                    }
                }
        );
        logger.info("ZK Cache fully started with listener.");

    }

    /**
     * Utility function to convert bytes to a cache entry.
     *
     * @param bytes
     *            to convert
     * @return the deserialized object
     * @throws IOException
     *             if poor formatting
     */
    public static PublisherCacheEntry fromBytes(final byte[] bytes)
            throws IOException {
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
                PublisherCacheEntry.SCHEMA$, new String(bytes));
        SpecificDatumReader<PublisherCacheEntry> reader = new SpecificDatumReader<PublisherCacheEntry>(
                PublisherCacheEntry.SCHEMA$);
        PublisherCacheEntry deserialized = reader.read(null, decoder);
        return deserialized;
    }

    /**
     * Close.
     */
    public synchronized void close() {
        if (INSTANCE == null)
            return;
        logger.info("Close invoked on ListenerZookeeperClient");
        CloseableUtils.closeQuietly(driverIdNode);
        CloseableUtils.closeQuietly(zkCache);
        CloseableUtils.closeQuietly(client);
        driverIdNode = null;
        zkCache = null;
        client = null;
    }

    /**
     * Utility function to convert a cache entry to bytes.
     *
     * @param entry
     *            to convert
     * @return the converted byte value of the entry
     * @throws IOException
     *             if something happens.
     */
    public static byte[] toBytes(final PublisherCacheEntry entry)
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

    /**
     * Add an entry to the cache. Will create the parent paths if needed.
     *
     * @pre No invalid parameters. Must have valid running ZK client instance.
     * @param entry
     *            to add.
     * @throws Exception
     *             when a ZK error occurs.
     */
    public void addEntry(final PublisherCacheEntry entry) throws Exception {
        Validate.notNull(client, "Client can't be null at this point");
        logger.info("Adding entry to Zookeeper cache=" + entry.toString());

        // Get the path we'll be making.
        String path = ZKPaths.makePath(this.zkRootDir, Long.toString(entry.getCampaignId()));
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

    /**
     * Iterates over cache entries. Skips over / logs invalid nodes.
     *
     * @pre No invalid parameters. Must have valid running ZK client instance.
     * @param callback
     *            to be invoked on each of the child path nodes.
     */
    public void iterate(final Consumer<PublisherCacheEntry> callback) {
        Validate.notNull(callback, "Callback can't be null");
        Validate.notNull(zkCache, "ZK-side cache cannot be null");
        logger.info("iterate called on cache size="
                + zkCache.getCurrentData().size());

        for (ChildData data : zkCache.getCurrentData()) {
            try {
                PublisherCacheEntry entry = fromBytes(data.getData());
                Validate.notNull(entry, "Entry shouldn't be null at this point");
                callback.accept(entry);
            } catch (IOException e) {
                logger.warn("Malformed Zookeeper entry found at path="
                        + data.getPath());
            }
        }
    }


}
