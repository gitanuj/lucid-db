package com.lucid.common;

import com.google.gson.Gson;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public final class Config {

    private static final String LOG_TAG = "CONFIG";

    private Config() {
    }

    public static final long TXN_ID_NOT_APPLICABLE = -1;
    public static int NUM_CLUSTERS;
    public static final List<AddressConfig> SERVER_IPS = new ArrayList<>();
    public static long READ_QUERY_TIMEOUT;
    public static long COMMAND_TIMEOUT;

    // SERVER_IPS list structure - contiguous blocks of shard replicas.
    static {

        try {
            Properties properties = new Gson().fromJson(new FileReader("lucid.config"), Properties.class);
            for (AddressConfig addressConfig : properties.getAddressConfigs()) {
                SERVER_IPS.add(addressConfig);
            }

            NUM_CLUSTERS = properties.getNumClusters();
            READ_QUERY_TIMEOUT = properties.getReadQueryTimeout();
            COMMAND_TIMEOUT = properties.getCommandTimeout();

        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Failed to load config", e);
        }
    }
}
