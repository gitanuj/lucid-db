package com.lucid.common;

import com.lucid.common.AddressConfig;

import java.util.*;

public final class Config {

    private Config() {
    }

    public static final int NUM_CLUSTERS = 3;
    public static final List<AddressConfig> SERVER_IPS = new ArrayList<>();
    public static final long TXN_ID_NOT_APPLICABLE = -1;
    public static final String DELIMITER = ";"; // Read responses to RC client are of the form "value; version"

    // SERVER_IPS list structure - contiguous blocks of shard replicas.
    static {
        String host = "127.0.0.1";

        SERVER_IPS.add(new AddressConfig(host, 9000, 9100, 9200));
        SERVER_IPS.add(new AddressConfig(host, 9001, 9101, 9201));
        SERVER_IPS.add(new AddressConfig(host, 9002, 9102, 9202));
        SERVER_IPS.add(new AddressConfig(host, 9003, 9103, 9203));
        SERVER_IPS.add(new AddressConfig(host, 9004, 9104, 9204));
        SERVER_IPS.add(new AddressConfig(host, 9005, 9105, 9205));
    }

    public static final long READ_QUERY_TIMEOUT = 10 * 1000;
    public static final long COMMAND_TIMEOUT = 5 * 1000;
}
