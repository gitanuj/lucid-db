import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class CopycatTestServer {

    public static void main(String[] args) throws Exception {
//        Utils.enableLogging();

        String host = InetAddress.getLocalHost().getHostName();
        int selfPort = Integer.valueOf(args[0]);
        Address selfAddress = new Address(host, selfPort);

        List<Address> members = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            members.add(new Address(host, Integer.valueOf(args[i])));
        }

        CopycatServer server = CopycatServer.builder(selfAddress, members)
                .withTransport(new NettyTransport())
                .withStateMachine(MapStateMachine::new)
                .withStorage(new Storage("logs/" + args[0] + System.currentTimeMillis()))
                .build();
        server.serializer().disableWhitelist();

        server.open().join();
    }
}
