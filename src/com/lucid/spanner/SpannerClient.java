package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;

import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
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

    public void executeCommand(Command command) throws UnexpectedCommand, LeaderNotFound{
        HashMap<String, Socket> sessionMap;
        Socket socket;
        Scanner reader;
        ObjectOutputStream writer;
        HashMap<String, String> commands;

        if(command instanceof WriteCommand) {
            sessionMap = new HashMap<>();
            commands = ((WriteCommand) command).getWriteCommands();
            for (Map.Entry entry : commands.entrySet()) {
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
                    throw new LeaderNotFound("Leader not found for key " + key);
            }

            try{
                for(Map.Entry<String, Socket> entry : sessionMap.entrySet()){
                    socket = entry.getValue();
                    writer = new ObjectOutputStream(socket.getOutputStream());
                    writer.writeObject(new TransportObject(((WriteCommand) command).getTxn_id(), entry.getKey(), commands.get(entry.getKey())));
                }
            }
            catch(Exception e){
                SpannerUtils.root.error(e.getMessage());
            }
        }
        else
           throw new UnexpectedCommand("Command not an instance of WriteCommand.");
    }
}