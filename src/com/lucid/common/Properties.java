package com.lucid.common;

import java.util.List;

public class Properties {

    private int numDatacenters;

    private List<AddressConfig> addressConfigs;

    private long readQueryTimeout;

    private long commandTimeout;

    private int SPANNER_CLIENT_TO_CLOSEST_DATACENTER_LATENCY;
    private int SPANNER_INTER_DATACENTER_LATENCY;
    private int RC_CLIENT_TO_DATACENTER_AVG_LATENCY;
    private int RC_INTER_DATACENTER_LATENCY;
    private int INTRA_DATACENTER_LATENCY;
    private int DETERMINE_SPANNER_LEADER_PING_LATENCY;
    private int COPYCAT_LOG_LEVEL;
    private int LUCID_LOG_LEVEL;

    private int LOCK_TABLE_SIZE;


    public int getDETERMINE_SPANNER_LEADER_PING_LATENCY() {
        return DETERMINE_SPANNER_LEADER_PING_LATENCY;
    }

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

    public int getNumDatacenters() {
        return numDatacenters;
    }

    public int getLockTableSize() {
        return LOCK_TABLE_SIZE;
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

    public LogUtils.LogLevel getCOPYCAT_LOG_LEVEL() {
        return LogUtils.LogLevel.getLogLevelById(COPYCAT_LOG_LEVEL);
    }

    public LogUtils.LogLevel getLUCID_LOG_LEVEL() {
        return LogUtils.LogLevel.getLogLevelById(LUCID_LOG_LEVEL);
    }
}
