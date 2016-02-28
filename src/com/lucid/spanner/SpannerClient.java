package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;

import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
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

    public String executeCommand(Command command) throws UnexpectedCommand, LeaderNotFound{
        HashMap<String, Socket> sessionMap;
        HashMap<String, String> commands;
        HashMap<Socket, Map<String, String>> commitObject = new HashMap<>();
        Socket socket, coordinatorSocket = null;
        Scanner reader;
        Address coordinatorAddress = null;
        ObjectOutputStream writer;

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
                        if (SpannerUtils.ROLE.values()[reader.nextInt()] == SpannerUtils.ROLE.LEADER) {
                            // Choose coordinator.
                            if(coordinatorSocket == null) {
                                coordinatorSocket = socket;
                                SpannerUtils.root.debug("SpannerClient --> Coordinator for transaction " + ((WriteCommand) command).getTxn_id() + " is " + coordinatorSocket.getInetAddress().getHostAddress());
                                coordinatorAddress = address;
                            }
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

            if(coordinatorSocket == null || coordinatorAddress == null)
                throw new NoCoordinatorException();

            // Prepare commit object for each leader
            for(Map.Entry<String, Socket> entry : sessionMap.entrySet()){
                Map<String, String> map = commitObject.get(socket);
                if(map != null)
                    map.put(entry.getKey(), commands.get(entry.getKey()));
                else {
                    map = new HashMap<>();
                    map.put(entry.getKey(), commands.get(entry.getKey()));
                    commitObject.put(entry.getValue(), map);
                }
            }

            // Send commit message to all leaders.
            try{
                for(Map.Entry<Socket, Map<String, String>> entry : commitObject.entrySet()){
                    socket = entry.getKey();
                    writer = new ObjectOutputStream(socket.getOutputStream());
                    writer.writeObject(new TransportObject(coordinatorAddress,((WriteCommand) command).getTxn_id(),
                            entry.getValue()));
                    if(socket != coordinatorSocket)
                        socket.close(); // Close connections to all leaders but the coordinator.
                }
            }
            catch(Exception e){
                SpannerUtils.root.error(e.getMessage());
            }

            // Wait for response from coordinator, and pass it on to caller.
            try{
                reader = new Scanner(new InputStreamReader(coordinatorSocket.getInputStream()));
                return reader.next();
            }
            catch(Exception e){
                SpannerUtils.root.error(e.getMessage());
                return "ABORT";
            }

        }
        else
           throw new UnexpectedCommand("Command not an instance of WriteCommand.");
    }
}