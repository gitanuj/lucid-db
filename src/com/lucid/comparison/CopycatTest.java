package com.lucid.comparison;

import com.lucid.common.LogUtils;
import com.lucid.common.Utils;
import com.lucid.test.PutCommand;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;

public class CopycatTest {

    private static int STARTING_PORT = 9000;

    // args[0]: Members in paxos cluster
    public static void main(String[] args) throws Exception {
        LogUtils.setCopycatLogLevel(LogUtils.LogLevel.ERROR);

        // Create addresses
        int clusterSize = Integer.parseInt(args[0]);
        final List<Address> addresses = new ArrayList<>();
        for (int i = 0; i < clusterSize; ++i) {
            addresses.add(new Address("127.0.0.1", STARTING_PORT + i));
        }

        // Start paxos members
        List<Thread> paxosServerThreads = new ArrayList<>();
        for (int i = 0; i < clusterSize; ++i) {
            final int index = i;
            Utils.startThreadWithName(() -> {
                CopycatServer copycatServer = CopycatServer.builder(addresses.get(index), addresses)
                        .withTransport(new NettyTransport())
                        .withStateMachine(CopycatStateMachine::new)
                        .withStorage(Storage.builder().withStorageLevel(StorageLevel.MEMORY).
                                withDirectory(String.valueOf(addresses.get(index).port())).build())
                        .build();
                copycatServer.serializer().disableWhitelist();
                copycatServer.onStateChange(new Listener<CopycatServer.State>() {
                    @Override
                    public void close() {

                    }

                    @Override
                    public void accept(CopycatServer.State state) {
                        System.out.println(index + ": " + state.name());
                    }
                });
                copycatServer.start().join();
            }, "server-" + i);
        }

        // Wait till all members are up
        for (Thread t : paxosServerThreads) {
            t.join();
        }
        // Just wait
        Utils.sleepQuietly(5000);

        // Create client
        CopycatClient copycatClient = CopycatClient.builder(addresses).withTransport(new NettyTransport()).build();
        copycatClient.serializer().disableWhitelist();
        copycatClient.connect().join();
        System.out.println("CopycatClient started");

        System.out.println("CopycatClient submitting");
        copycatClient.submit(new PutCommand(null, null)).join();
        System.out.println("CopycatClient done");
    }
}
