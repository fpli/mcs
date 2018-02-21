package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.common.SnapshotId;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHelper {
    /** Private Zookeeper testing instance. We want to use one instance because otherwise the same testing classes will all use the same ZK server. */
    private static TestingServer ZK_SERVER = null;
    /** Return a positive long */
    public synchronized static TestingServer initializeZkServer() throws Exception {
        // Important here:
        // Use just one ZK instance. If each test class sets up their own individual server,
        // then the complexity grows exponentially as the test servers turn into a cluster.
        // Each test really runs only 1 server, so let's make sure they all share the same server.
        //
        // I realize this means that multiple instances will call close(), but that should be harmless.
        if (ZK_SERVER == null) {

            // Create a guaranteed unique temp dir.
            File zkTempDir = File.createTempFile("zk-logs", Long.toString(SnapshotId.getNext(1).getRepresentation()));
            assertTrue(zkTempDir.delete()); // Delete the file at the directory.

            // This should never fail, because in theory the snapshot ID should create a guaranteed unique directory.
            assertTrue(!zkTempDir.exists());

            // It's a temporary directory, so we should succeed here.
            assertTrue(zkTempDir.mkdir());

            // Mark that we want this directory to die when the JVM does.
            zkTempDir.deleteOnExit();

            ZK_SERVER = new TestingServer(InstanceSpec.getRandomPort(), zkTempDir);
        }
        return ZK_SERVER;
    }

    public static void terminateZkServer() {
        try {
            ZK_SERVER.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            ZK_SERVER = null;
        }
    }
}
