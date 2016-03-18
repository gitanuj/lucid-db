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
    public static int SPANNER_CLIENT_TO_CLOSEST_DATACENTER_LATENCY;
    public static int SPANNER_INTER_DATACENTER_LATENCY;
    public static int RC_CLIENT_TO_DATACENTER_AVG_LATENCY;
    public static int RC_INTER_DATACENTER_LATENCY;
    public static int INTRA_DATACENTER_LATENCY;
    public static int LOCK_TABLE_SIZE;
    public static int SPANNER = 0;
    public static int DETERMINE_SPANNER_LEADER_PING_LATENCY;

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
            LogUtils.setCopycatLogLevel(properties.getCOPYCAT_LOG_LEVEL());
            LogUtils.setLucidLogLevel(properties.getLUCID_LOG_LEVEL());

            SPANNER_CLIENT_TO_CLOSEST_DATACENTER_LATENCY = properties.getSPANNER_CLIENT_TO_CLOSEST_DATACENTER_LATENCY();
            SPANNER_INTER_DATACENTER_LATENCY = properties.getSPANNER_INTER_DATACENTER_LATENCY();
            RC_CLIENT_TO_DATACENTER_AVG_LATENCY = properties.getRC_CLIENT_TO_DATACENTER_AVG_LATENCY();
            RC_INTER_DATACENTER_LATENCY = properties.getRC_INTER_DATACENTER_LATENCY();
            INTRA_DATACENTER_LATENCY = properties.getINTRA_DATACENTER_LATENCY();
            DETERMINE_SPANNER_LEADER_PING_LATENCY = properties.getDETERMINE_SPANNER_LEADER_PING_LATENCY();
            LOCK_TABLE_SIZE = properties.getLockTableSize();

        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Failed to load config", e);
        }
    }
}
