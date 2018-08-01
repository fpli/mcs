package com.ebay.app.raptor.chocolate.common;

// import java classes

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

// import zookeeper classes

public class ZooKeeperConnection {

    // declare zookeeper instance to access ZooKeeper ensemble
    private ZooKeeper zoo = null;
    final CountDownLatch connectedSignal = new CountDownLatch(1);

    // Method to connect zookeeper ensemble.
    public ZooKeeper connect(String host) throws IOException,InterruptedException {

        zoo = new ZooKeeper(host,5000, new Watcher() {

            public void process(WatchedEvent we) {

                if (we.getState() == KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });

        connectedSignal.await();
        return zoo;
    }

    public String getStringData(String path) {
        if (null == this.zoo) {
            return null;
        }

        try {
            Stat stat = zoo.exists(path, false);
            if (null == stat) {
                return null;
            }

            byte[] bn = zoo.getData(path, false, null);
            return new String(bn, "UTF-8");
        } catch (Exception e) {
            return null;
        }
    }

    // Method to disconnect from zookeeper server
    public void close() throws InterruptedException {
        zoo.close();
    }
}