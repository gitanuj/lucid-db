package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

import java.util.*;

public final class Config {

    private Config() {
    }

    public static final int NUM_CLUSTERS = 2;

    public static final List<AddressConfig> SERVER_IPS = new ArrayList<>();
    public static final List<Integer> PAXOS_PORTS = new ArrayList<>();
    public static final List<Integer> SERVER_PORTS = new ArrayList<>();

    static {
        SERVER_IPS.add(new Address("localhost", 8888));
        SERVER_IPS.add(new Address("localhost", 8888));
        SERVER_IPS.add(new Address("localhost", 8888));
        SERVER_IPS.add(new Address("localhost", 8888));
        PAXOS_PORTS.add(9100);
        PAXOS_PORTS.add(9101);
        PAXOS_PORTS.add(9102);
        PAXOS_PORTS.add(9103);
        SERVER_PORTS.add(9200);
        SERVER_PORTS.add(9201);
        SERVER_PORTS.add(9202);
        SERVER_PORTS.add(9203);
    }

    public static final long READ_QUERY_TIMEOUT = 5 * 1000;
    public static final long COMMAND_TIMEOUT = 5 * 1000;
}
