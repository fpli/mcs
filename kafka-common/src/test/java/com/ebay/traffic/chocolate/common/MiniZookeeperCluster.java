package com.ebay.traffic.chocolate.common;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by yliu29 on 2/28/18.
 *
 * A light-weight mini zookeeper cluster, which only contains 1 quorum server.
 *
 * The port listened on is random, or can be specified explicitly.
 */
public class MiniZookeeperCluster {

  private NIOServerCnxnFactory factory;

  private final int port;
  private String address;

  private boolean started = false;
  private boolean shutdown = false;

  private final String home;

  public MiniZookeeperCluster() {
    this(null);
  }

  public MiniZookeeperCluster(String dir) {
    this(TestHelper.getRandomPort(), dir);
  }

  public MiniZookeeperCluster(int port, String dir) {
    this.port = port;
    try {
      if (dir == null) {
        home = TestHelper.createTempDir("zookeeper").getCanonicalPath();
      } else {
        home = TestHelper.createTempDir(dir, "zookeeper").getCanonicalPath();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Start the zookeeper server.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    if (started) {
      return;
    }
    started = true;

    address = "127.0.0.1:" + port;
    File snapshotDir = TestHelper.createTempDir(home, "snapshot");
    File logDir = TestHelper.createTempDir(home, "log");
    factory = new NIOServerCnxnFactory();
    String hostname = address.split(":")[0];
    int port = Integer.valueOf(address.split(":")[1]);
    factory.configure(new InetSocketAddress(hostname, port), 1024);
    try {
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, 1000));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Return the connection string of zookeeper, e.g. 127.0.0.1:1234
   *
   * @return connection string
   */
  public String getConnectionString() {
    return address;
  }

  /**
   * Create new <code>ZkClient</code>
   *
   * @return zkClient
   */
  public ZkClient newZkClient() {
    return new ZkClient(address, 5000, 5000, new ZkStringSerializer());
  }

  /**
   * Shutdown the zookeeper server.
   *
   * @throws IOException
   */
  public void shutdown() throws IOException {
    if (!started) {
      return;
    }
    shutdown = true;
    started = false;

    if (factory != null) {
      factory.shutdown();
      factory = null;
    }
  }

  private static class ZkStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      return ((String) data).getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      return bytes != null ? new String(bytes) : null;
    }
  }
}
