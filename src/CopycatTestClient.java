import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class CopycatTestClient {

    public static void main(String[] args) throws Exception {
        Utils.enableLogging();

        String host = InetAddress.getLocalHost().getHostName();
        List<Address> members = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            members.add(new Address(host, Integer.valueOf(args[i])));
        }

        CopycatClient client = CopycatClient.builder(members)
                .withTransport(new NettyTransport())
                .build();

        client.open().join();

        client.submit(new PutCommand("foo", "Hello world!")).get();

        System.out.println(client.submit(new GetQuery("foo")).get());

        client.close().join();
    }
}
