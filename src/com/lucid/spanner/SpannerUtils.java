package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;

import java.util.List;

public class SpannerUtils {

    public static CopycatClient buildClient(List<Address> members) {
        CopycatClient client = CopycatClient.builder(members)
                .withTransport(new NettyTransport())
                .build();
        client.serializer().disableWhitelist();
        return client;
    }

    public static List<Address> getClusterIPs(Object key) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        int index = key.hashCode() % Config.NUM_CLUSTERS;
        return Config.SERVER_IPS.subList(index * clusterSize, index * clusterSize + clusterSize);
    }
}
