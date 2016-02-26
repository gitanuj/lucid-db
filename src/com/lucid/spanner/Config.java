package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

import java.util.ArrayList;
import java.util.List;

public final class Config {

    private Config() {
    }

    public static final int NUM_CLUSTERS = 2;

    public static final List<Address> SERVER_IPS = new ArrayList<>();

    static {
        SERVER_IPS.add(new Address("localhost", 8888));
        SERVER_IPS.add(new Address("localhost", 8888));
        SERVER_IPS.add(new Address("localhost", 8888));
        SERVER_IPS.add(new Address("localhost", 8888));
    }

    public static final long READ_QUERY_TIMEOUT = 5 * 1000;
}
