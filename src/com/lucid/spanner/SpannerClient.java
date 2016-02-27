package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.net.Socket;
import java.io.InputStreamReader;

public class SpannerClient {
    private int selfPort;
    public SpannerClient(int port){
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

    public void executeCommand(Command command) throws UnexpectedCommand, LeaderNotFound{
        // Coordinator cluster
//        CopycatClient coordinatorClient = SpannerUtils.buildClient(SpannerUtils.getClusterIPs(commands.get(0)));
//        coordinatorClient.open().join();
//        coordinatorClient.close().join();
        HashMap<String, Socket> sessionMap = new HashMap<>();
        Socket socket;
        Scanner reader;
        Set<Map.Entry<String, Object>> commands;
        if(command instanceof WriteCommand) {
            commands = ((WriteCommand) command).getWriteCommands().entrySet();
            for (Map.Entry entry : commands) {
                String key = (String)entry.getKey();
                for (Address address : SpannerUtils.getClusterIPs(key)) {
                    try {
                        socket = new Socket(address.host(), address.port());
                        reader = new Scanner(new InputStreamReader(socket.getInputStream()));
                        if (SpannerUtils.ROLE.values()[reader.nextInt()] == SpannerUtils.ROLE.LEADER) {
                            sessionMap.put(key, socket);
                        }
                        else
                            socket.close();
                    } catch (Exception e) {
                        SpannerUtils.root.error(e.getMessage());
                    }
                }
                if (sessionMap.get(key) == null)
                    throw new LeaderNotFound("Leader not found for key " + command);
            }

            for(Map.Entry entry : commands){
                Address address =
                socket = new Socket()
            }
        }
        else
           throw new UnexpectedCommand("Command not an instance of WriteCommand.");
    }
}
