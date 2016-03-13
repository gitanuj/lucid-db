package com.lucid.common;

import java.util.List;

public class Properties {

    private int numClusters;

    private List<AddressConfig> addressConfigs;

    private long readQueryTimeout;

    private long commandTimeout;

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
