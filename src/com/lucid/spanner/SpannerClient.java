package com.lucid.spanner;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SpannerClient {

    private List<Command> commands;

    private Query query;

    public List<Command> getCommands() {
        return commands;
    }

    public void setCommands(List<Command> commands) {
        this.commands = commands;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void execute() {
        if (query != null) {
            try {
                Object result = executeQuery(query);
                System.out.println("GetQuery result: " + result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (commands != null) {
            executeCommands(commands);
        }
    }

    private Object executeQuery(Query query) throws InterruptedException, ExecutionException, TimeoutException {
        // TODO Read from specific cluster?
        CopycatClient client = SpannerUtils.buildClient(Config.SERVER_IPS);
        client.open().join();
        Object result = client.submit(query).get(Config.READ_QUERY_TIMEOUT, TimeUnit.MILLISECONDS);
        client.close().join();
        return result;
    }

    private void executeCommands(List<Command> commands) {
        // Coordinator cluster
        CopycatClient coordinatorClient = SpannerUtils.buildClient(SpannerUtils.getClusterIPs(commands.get(0)));
        coordinatorClient.open().join();
        coordinatorClient.close().join();
    }
}
