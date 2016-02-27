package com.lucid.spanner;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SpannerClient {
    private int selfPort;
    SpannerClient(int port){
        this.selfPort = port;
    }

    public Object executeQuery(Query query) throws InterruptedException, ExecutionException, TimeoutException {
        // TODO Read from specific cluster?
        CopycatClient client = SpannerUtils.buildClient(Config.SERVER_IPS);
        client.open().join();
        Object result = client.submit(query).get(Config.READ_QUERY_TIMEOUT, TimeUnit.MILLISECONDS);
        client.close().join();
        return result;
    }

    public void executeCommands(List<Command> commands) {
        // Coordinator cluster
//        CopycatClient coordinatorClient = SpannerUtils.buildClient(SpannerUtils.getClusterIPs(commands.get(0)));
//        coordinatorClient.open().join();
//        coordinatorClient.close().join();

        for(Command command : commands){

        }

    }
}
