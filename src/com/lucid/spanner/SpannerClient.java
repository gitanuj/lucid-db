package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;

import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.net.Socket;
import java.io.InputStreamReader;

public class SpannerClient {

    public String executeQuery(Query query) throws InterruptedException, ExecutionException, TimeoutException {
        CopycatClient client = SpannerUtils.buildClient(Config.SERVER_IPS);
        client.open().join();
        String result = (String)client.submit(query).get(Config.READ_QUERY_TIMEOUT, TimeUnit.MILLISECONDS);
        client.close().join();
        return result;
    }

    public boolean executeCommand(Command command) throws UnexpectedCommand, LeaderNotFound{
        HashMap<String, Socket> sessionMap;
        Socket socket, coordinatorSocket = null;
        Scanner reader;
        ObjectOutputStream writer;
        HashMap<String, String> commands;

        if(command instanceof WriteCommand) {

            // Determine leaders.
            sessionMap = new HashMap<>();
            commands = ((WriteCommand) command).getWriteCommands();
            for (Map.Entry entry : commands.entrySet()) {
                String key = (String)entry.getKey();
                for (Address address : SpannerUtils.getClusterIPs(key)) {
                    try {
                        socket = new Socket(address.host(), address.port());
                        reader = new Scanner(new InputStreamReader(socket.getInputStream()));
                        if (SpannerUtils.ROLE.LEADER.equals(reader.nextLine())) { // TODO Clarify how to handle this
                            SpannerUtils.root.debug("SpannerClient --> Leader for " + key + " is " + address.host());
                            sessionMap.put(key, socket);
                        }
                        else
                            socket.close();
                    } catch (Exception e) {
                        SpannerUtils.root.error(e.getMessage());
                    }
                }
                if (sessionMap.get(key) == null)
                    throw new LeaderNotFound("Leader not found for key " + key);
            }
            // Choose coordinator to be the first entry in session map.
            // Send commit message to all leaders.
            try{
                for(Map.Entry<String, Socket> entry : sessionMap.entrySet()){
                    socket = entry.getValue();
                    if(coordinatorSocket == null) {
                        coordinatorSocket = socket;
                        SpannerUtils.root.debug("SpannerClient --> Coordinator for transaction " + ((WriteCommand) command).getTxn_id() + " is " + coordinatorSocket.getInetAddress().getHostAddress());
                    }
                    writer = new ObjectOutputStream(socket.getOutputStream());
                    writer.writeObject(new TransportObject(coordinatorSocket.getInetAddress(),((WriteCommand) command).getTxn_id(), entry.getKey(), commands.get(entry.getKey())));
                }
            }
            catch(Exception e){
                SpannerUtils.root.error(e.getMessage());
            }

            // TODO Wait for response from coordinator.
        }
        else
           throw new UnexpectedCommand("Command not an instance of WriteCommand.");
    }
}