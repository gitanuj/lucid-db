package com.lucid.test;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CopycatTestClient {

    public static void main(String[] args) throws Exception {
//        Utils.enableLogging();

        String host = InetAddress.getLocalHost().getHostName();
        List<Address> members = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            members.add(new Address(host, Integer.valueOf(args[i])));
        }

        CopycatClient client = CopycatClient.builder(members)
                .withTransport(new NettyTransport())
                .build();
        client.serializer().disableWhitelist();

        client.connect().join();

        client.submit(new PutCommand("foo", "Hello world!")).get(5, TimeUnit.SECONDS);

        System.out.println(client.submit(new GetQuery("foo")).get(5, TimeUnit.SECONDS));

        client.close().join();
    }
}
