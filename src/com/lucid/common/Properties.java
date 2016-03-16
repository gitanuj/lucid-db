package com.lucid.common;

import java.util.List;

public class Properties {

    private int numClusters;

    private List<AddressConfig> addressConfigs;

    private long readQueryTimeout;

    private long commandTimeout;

    private int SPANNER_CLIENT_TO_CLOSEST_DATACENTER_LATENCY;
    private int SPANNER_INTER_DATACENTER_LATENCY;
    private int RC_CLIENT_TO_DATACENTER_AVG_LATENCY;
    private int RC_INTER_DATACENTER_LATENCY;
    private int INTRA_DATACENTER_LATENCY;

    public int getRC_INTER_DATACENTER_LATENCY() {
        return RC_INTER_DATACENTER_LATENCY;
    }

    public int getSPANNER_CLIENT_TO_CLOSEST_DATACENTER_LATENCY() {
        return SPANNER_CLIENT_TO_CLOSEST_DATACENTER_LATENCY;
    }

    public int getRC_CLIENT_TO_DATACENTER_AVG_LATENCY() {
        return RC_CLIENT_TO_DATACENTER_AVG_LATENCY;
    }

    public int getSPANNER_INTER_DATACENTER_LATENCY() {
        return SPANNER_INTER_DATACENTER_LATENCY;
    }

    public int getINTRA_DATACENTER_LATENCY() {
        return INTRA_DATACENTER_LATENCY;
    }

    public int getNumClusters() {
        return numClusters;
    }

    public List<AddressConfig> getAddressConfigs() {
        return addressConfigs;
    }

    public long getReadQueryTimeout() {
        return readQueryTimeout;
    }

    public long getCommandTimeout() {
        return commandTimeout;
    }
}
