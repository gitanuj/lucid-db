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

    public static List<AddressConfig> getReplicaClusterIPs(Object key) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        int index = Math.abs(key.hashCode()) % Config.NUM_CLUSTERS;
        return Config.SERVER_IPS.subList(index * clusterSize, index * clusterSize + clusterSize);
    }

    public static int getReplicaClusterID(Object key) {
        int index = Math.abs(key.hashCode()) % Config.NUM_CLUSTERS;
        return index;
    }

    public static List<AddressConfig> getReplicaClusterIPs(int index) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        return Config.SERVER_IPS.subList(index * clusterSize, index * clusterSize + clusterSize);
    }

    public static Thread startThreadWithName(Runnable runnable, String name) {
        Thread thread = new Thread(runnable);
        thread.setName(name);
        thread.start();
        return thread;
    }

    public static List<AddressConfig> getDatacenterIPs(int index) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        int offset = index % clusterSize;

        List<AddressConfig> list = new ArrayList<>();
        for (int i = offset; i < Config.SERVER_IPS.size(); i += Config.NUM_CLUSTERS) {
            list.add(Config.SERVER_IPS.get(i));
        }
        return list;
    }

    public static List<Integer> getDatacenterIndexes(int datacenterID) {
        List<Integer> list = new ArrayList<>();

        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        int offset = datacenterID % clusterSize;

        for (int i = offset; i < Config.SERVER_IPS.size(); i += clusterSize) {
            list.add(i);
        }
        return list;
    }

    public static List<AddressConfig> getReplicaIPs(int index) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS; // Note: Assuming equal-sized clusters
        int position = index / clusterSize;
        return Config.SERVER_IPS.subList(position * clusterSize, position * clusterSize + clusterSize);
    }

    public static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Failed to sleep", e);
        }
    }
}
