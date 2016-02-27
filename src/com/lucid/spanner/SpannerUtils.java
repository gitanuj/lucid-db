package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

public class SpannerUtils {

    public static enum ROLE{
        COORDINATOR, LEADER, COHORT
    }

    public static enum SERVER_MSG{
        PREPAREACK, COMMIT
    }

    public static ch.qos.logback.classic.Logger root;

    public static void enableLogging() {
        setLoggingLevel(ch.qos.logback.classic.Level.DEBUG);
    }

    public static void setLoggingLevel(ch.qos.logback.classic.Level level) {
        root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(level);
    }

    public static CopycatClient buildClient(List<Address> members) {
        CopycatClient client = CopycatClient.builder(members)
                .withTransport(new NettyTransport())
                .build();
        client.serializer().disableWhitelist();
        return client;
    }

    public static CopycatServer buildServer(Address selfAddress, List<Address> members) {
        CopycatServer server = CopycatServer.builder(selfAddress, members)
                .withTransport(new NettyTransport())
                .withStateMachine(SpannerStateMachine::new)
                .withStorage(new Storage("logs/" + selfAddress))
                .build();
        server.serializer().disableWhitelist();
        return server;
    }

    public static List<Address> getClusterIPs(Object key) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        int index = key.hashCode() % Config.NUM_CLUSTERS;
        return Config.SERVER_IPS.subList(index * clusterSize, index * clusterSize + clusterSize);
    }

    public static boolean isThisMyIpAddress(String host, int port) {
        Address addr = new Address(host, port);
        // TODO: check if remote address is own ip address
        if (addr.socketAddress().getAddress().isAnyLocalAddress()
                || addr.socketAddress().getAddress().isLoopbackAddress())
            return true;
        return false;
    }

    public static Thread startThreadWithName(Runnable runnable, String name)
    {
        Thread thread = new Thread(runnable);
        thread.setName(name);
        thread.start();
        return thread;
    }
}
