package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class SpannerUtils {

    public static enum ROLE{
        COORDINATOR, LEADER, COHORT
    }

    public enum SERVER_MSG{
        PREPARE_ACK, PREPARE_NACK, COMMIT, ABORT
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

    public static int getClusterID(Object key){
        int index = key.hashCode() % Config.NUM_CLUSTERS;
        return index;
    }

    public static List<Address> getClusterIPs(Object key) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        int index = key.hashCode() % Config.NUM_CLUSTERS;
        return Config.SERVER_IPS.subList(index * clusterSize, index * clusterSize + clusterSize);
    }

    public static List<Address> getClusterIPs(int index) {
        int clusterSize = Config.SERVER_IPS.size() / Config.NUM_CLUSTERS;
        return Config.SERVER_IPS.subList(index * clusterSize, index * clusterSize + clusterSize);
    }

    // Just checks is my ip is equal to given host, no port matching
    public static boolean isThisMyIpAddress(String host)  {
        if(host=="localhost")
            return true;

        if (host==getMyInternetIP()){
            return true;
        }

        return false;
    }

    public static String getMyInternetIP(){
        BufferedReader in = null;
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

    public static int getMyPaxosAddressIndex(String host, int port){
        int index = -1;
        for(Address addr: Config.SERVER_IPS){
            index++;
            if(isThisMyIpAddress(addr.host()) && port==addr.port()){
                break;
            }
        }
        if(index>=Config.SERVER_IPS.size()){
            return -1;
        }
        return index;
    }

    public static List<Address> getPaxosCluster(int index){
        List<Address> paxosMembers = new ArrayList<Address>();
        int clusterSize = Config.SERVER_IPS.size()/Config.NUM_CLUSTERS; // Note: Assuming equal-sized clusters
        int position = index % clusterSize;

        for (int i=position; i<Config.SERVER_IPS.size(); i += clusterSize) {
            if(i!=index){
                paxosMembers.add(new Address(Config.SERVER_IPS.get(i)));
            }
        }
        return paxosMembers;
    }

    public static List<Address> getPaxosClusterAll(int index){
        List<Address> paxosMembers = new ArrayList<Address>();
        int clusterSize = Config.SERVER_IPS.size()/Config.NUM_CLUSTERS; // Note: Assuming equal-sized clusters
        int position = index % clusterSize;

        for (int i=position; i<Config.SERVER_IPS.size(); i += clusterSize) {
                paxosMembers.add(new Address(Config.SERVER_IPS.get(i)));
        }
        return paxosMembers;
    }

    public static Thread startThreadWithName(Runnable runnable, String name)
    {
        Thread thread = new Thread(runnable);
        thread.setName(name);
        thread.start();
        return thread;
    }
}
