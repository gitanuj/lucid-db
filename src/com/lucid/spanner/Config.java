package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

import java.util.*;

public final class Config {

    private Config() {
    }

    public static final int NUM_CLUSTERS = 2;

    public static final List<Address> SERVER_IPS = new ArrayList<>();

    static {
        SERVER_IPS.add(new AddressConfig("localhost", 8888, 9100, 9200));
        SERVER_IPS.add(new AddressConfig("localhost", 8888, 9101, 9201));
        SERVER_IPS.add(new AddressConfig("localhost", 8888, 9102, 9202));
        SERVER_IPS.add(new AddressConfig("localhost", 8888, 9103, 9203));
    }

    public static final long READ_QUERY_TIMEOUT = 5 * 1000;
    public static final long COMMAND_TIMEOUT = 5 * 1000;
}
