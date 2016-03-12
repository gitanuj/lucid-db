package com.lucid.spanner;

import com.lucid.common.AddressConfig;
import com.lucid.common.Config;
import com.lucid.common.Utils;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class SpannerUtils {

    public enum SERVER_MSG {
        PREPARE_ACK, PREPARE_NACK, COMMIT, ABORT
    }

    public static CopycatClient buildClient(List<Address> members) {
        CopycatClient client = CopycatClient.builder(members)
                .withTransport(new NettyTransport())
                .build();
        client.serializer().disableWhitelist();
        return client;
    }

    // Just checks is my ip is equal to given host, no port matching
    public static boolean isThisMyIpAddress(String host) {
        if (host.equals("localhost"))
            return true;

        if (host.equals(getMyInternetIP())) {
            return true;
        }
        return false;
    }

    public static String getMyInternetIP() {
        BufferedReader in;
        String ip = "";
        try {
            URL whatismyip = new URL("http://checkip.amazonaws.com");
            in = new BufferedReader(new InputStreamReader(
                    whatismyip.openStream()));
            ip = in.readLine(); //you get the IP as a String

        } catch (IOException e) {
            e.printStackTrace();
        }
        return ip;
    }

    public static List<AddressConfig> getPaxosClusterAll(int index) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS; // Note: Assuming equal-sized clusters
        int position = index / clusterSize;
        return Utils.getReplicaClusterIPs(position);
    }

    public static List<Address> toAddress(List<AddressConfig> addressConfigList) {
        List<Address> addressList = new ArrayList<>();
        for (AddressConfig addressConfig : addressConfigList) {
            addressList.add(addressConfig.toAddress());
        }
        return addressList;
    }
}
