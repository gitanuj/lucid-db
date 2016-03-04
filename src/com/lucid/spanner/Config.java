package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

import java.net.InetAddress;
import java.util.*;

public final class Config {

    private Config() {
    }

    public static final int NUM_CLIENTS = 100;
    public static final int NUM_CLUSTERS = 1;
    public static final List<AddressConfig> SERVER_IPS = new ArrayList<>();

    static {
        String host = "169.231.64.24";
//        try {
//            host = InetAddress.getLocalHost().getHostName();
//        } catch (Exception e) {}

        SERVER_IPS.add(new AddressConfig(host, 9000, 9100, 9200));
        SERVER_IPS.add(new AddressConfig(host, 9001, 9101, 9201));
        //SERVER_IPS.add(new AddressConfig("localhost", 9002, 9102, 9202));
        //SERVER_IPS.add(new AddressConfig("localhost", 9003, 9103, 9203));
    }

    public static final long READ_QUERY_TIMEOUT = 10 * 1000;
    public static final long COMMAND_TIMEOUT = 5 * 1000;
}
