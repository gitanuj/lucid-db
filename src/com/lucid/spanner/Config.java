package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import javafx.util.Pair;

import java.util.*;




public final class Config {

    private Config() {
    }

    public static final int NUM_CLUSTERS = 2;

    //public static final List<Address> SERVER_IPS = new ArrayList<>();
    // Server Address (includes Client Port), Paxos Port, Server Port
    public static final Map<Address, <Integer, Integer>> SERVER_IPS = new HashMap<>();
    public static final List<Integer> PAXOS_PORTS = new ArrayList<>();
    public static final List<Integer> SERVER_PORTS = new ArrayList<>();

    static {
        SERVER_IPS.put(new Address("localhost", 8000), (Map)new HashMap<>().put(9100, 9200));
        SERVER_IPS.put(new Address("localhost", 8001), (Map)new HashMap<>().put(9101, 9201));
        SERVER_IPS.put(new Address("localhost", 8002), (Map)new HashMap<>().put(9102, 9202));
        SERVER_IPS.put(new Address("localhost", 8003), (Map)new HashMap<>().put(9103, 9203));
    }

    public static final long READ_QUERY_TIMEOUT = 5 * 1000;
    public static final long COMMAND_TIMEOUT = 5 * 1000;
}
