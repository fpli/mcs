package com.ebay.app.raptor.chocolate.common;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Created by spugach on 2/1/17.
 */
public class ZKUploader {
    public static void main(String [] args) {
        if (args.length != 3) {
            System.out.println("Usage: java ZKUploader <ZK server> <ZK path> <file>");
            return;
        }

        String zkServer = args[0];
        String zkPath = args[1];
        String fileName = args[2];

        ZooKeeperConnection connection = new ZooKeeperConnection();
        try {
            System.out.println(String.format("Connecting to %s", zkServer));
            ZooKeeper zoo = connection.connect(zkServer);
            System.out.println(String.format("Connected to %s", zkServer));
            Stat stat = zoo.exists(zkPath, false);
            if (stat != null) {
                if (stat.getNumChildren() > 0) {
                    System.out.println("Must be a leaf node");
                    return;
                }
            } else {
                System.out.println(String.format("ZNode (%s) not found", zkPath));
                return;
            }

            byte[] bytes = Files.readAllBytes(Paths.get(fileName));
            zoo.setData(zkPath, bytes, stat.getVersion());

            System.out.println(String.format("Uploaded %d bytes", bytes.length));
            connection.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
