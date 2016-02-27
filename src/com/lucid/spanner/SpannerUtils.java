package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

import java.util.List;

public class SpannerUtils {

    public static enum ROLE{
        COORDINATOR, LEADER,   COHORT
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
}
