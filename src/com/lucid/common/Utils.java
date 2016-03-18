package com.lucid.common;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    private static final String LOG_TAG = "UTILS";

    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong while closing stream", e);
            }
        }
    }

    public static List<AddressConfig> getReplicaIPs(int index) {
        int shardID = index / Config.NUM_DATACENTERS;
        return getReplicaClusterIPs(shardID);
    }

    public static List<AddressConfig> getReplicaClusterIPs(Object key) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_DATACENTERS;
        int index = Math.abs(key.hashCode()) % Config.NUM_DATACENTERS;
        return Config.SERVER_IPS.subList(index * clusterSize, index * clusterSize + clusterSize);
    }

    public static int getShardID(Object key) {
        int numShards = Config.SERVER_IPS.size() / Config.NUM_DATACENTERS;
        int index = Math.abs(key.hashCode()) % numShards;
        return index;
    }

    public static List<AddressConfig> getReplicaClusterIPs(int shardID) {
        return Config.SERVER_IPS.subList(shardID * Config.NUM_DATACENTERS, shardID * Config.NUM_DATACENTERS + Config.NUM_DATACENTERS);
    }

    public static Thread startThreadWithName(Runnable runnable, String name) {
        Thread thread = new Thread(runnable);
        thread.setName(name);
        thread.start();
        return thread;
    }

    public static int getDatacenterID(int index) {
        return index % Config.NUM_DATACENTERS;
    }

    public static List<AddressConfig> getDatacenterIPs(int index) {
        int datacenterID = getDatacenterID(index);

        List<AddressConfig> list = new ArrayList<>();
        for (Integer i : getDatacenterIndexes(datacenterID)) {
            list.add(Config.SERVER_IPS.get(i));
        }
        return list;
    }

    public static List<Integer> getDatacenterIndexes(int datacenterID) {
        List<Integer> list = new ArrayList<>();
        int numShards = Config.SERVER_IPS.size() / Config.NUM_DATACENTERS;
        for (int i = 0; i < numShards; ++i) {
            list.add(datacenterID + i * Config.NUM_DATACENTERS);
        }
        return list;
    }

    public static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Failed to sleep", e);
        }
    }

    public static <T> void printList(List<T> list) {
        System.out.println("---list---");
        for (T item : list) {
            System.out.println(item);
        }
        System.out.println("------");
    }
}
