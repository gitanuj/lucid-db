package com.lucid.test;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CopycatTestServer {

    public static void main(String[] args) throws Exception {
//        Utils.enableLogging();

        String host = InetAddress.getLocalHost().getHostName();
        int index = Integer.valueOf(args[0]);
        int selfPort = Integer.valueOf(args[1]);
        Address selfAddress = new Address(host, selfPort);

        List<Address> members = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            members.add(new Address(host, Integer.valueOf(args[i])));
        }

        CopycatServer server = CopycatServer.builder(selfAddress, members)
                .withTransport(new NettyTransport())
                .withStateMachine(MapStateMachine::new)
                .withStorage(Storage.builder().withStorageLevel(StorageLevel.MEMORY).
                        withDirectory(String.valueOf(selfPort)).build())
                .build();

        server.serializer().disableWhitelist();

        server.open().join();

        if(index==0) {
            System.out.println("\nStarting client............\n");
            Runnable clientRunnable = () -> {
                try {
                    for (int i = 0; i < args.length; i++) {
                        members.add(new Address(host, Integer.valueOf(args[i])));
                    }

                    CopycatClient client = CopycatClient.builder(members)
                            .withTransport(new NettyTransport())
                            .build();
                    client.serializer().disableWhitelist();

                    client.open().join();

                    client.submit(new PutCommand("foo", "Hello world!")).get(5, TimeUnit.SECONDS);

                    System.out.println(client.submit(new GetQuery("foo")).get(5, TimeUnit.SECONDS));

                    client.close().join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
            Thread clientThread = new Thread(clientRunnable);
            clientThread.start();
        }
    }
}
