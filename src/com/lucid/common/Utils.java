package com.lucid.common;

import java.io.Closeable;
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
}
