package com.lucid.spanner;

import com.lucid.common.LogUtils;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;

import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SpannerClient implements YCSBClient {

    private static final String LOG_TAG = "SPANNER_CLIENT";

    private CopycatClient copycatClient;

    public SpannerClient() {
        copycatClient = SpannerUtils.buildClient(SpannerUtils.toAddress(Config.SERVER_IPS));
        copycatClient.open().join();
    }

    @Override
    public String executeQuery(Query query) throws InterruptedException, ExecutionException, TimeoutException {
        return (String) copycatClient.submit(query).get(Config.READ_QUERY_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean executeCommand(Command command) throws UnexpectedCommand {

        if (command instanceof WriteCommand) {
            try {
                return doExecuteCommand(command);
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong.", e);
            }
        } else
            throw new UnexpectedCommand(LOG_TAG + "Command not an instance of WriteCommand.");

        return false;
    }

    private boolean doExecuteCommand(Command command) throws LeaderNotFound, NoCoordinatorException {
        HashMap<Integer, Socket> sessionMap; // Maps cluster IDs to leader in cluster.
        Map<String, String> commands; // Key - Value map.
        HashMap<Socket, Map<String, String>> commitObject; // Maps leaders to map of key-value
        // pairs of objects to commit in that cluster.
        HashMap<Integer, List<String>> sMap; // Maps cluster IDs to keys.
        Socket socket, coordinatorSocket = null;
        Scanner reader;
        AddressConfig coordinatorAddress = null;
        ObjectOutputStream writer;

        sessionMap = new HashMap<>();
        commands = ((WriteCommand) command).getWriteCommands();
        sMap = new HashMap<>();
        commitObject = new HashMap<>();

        // Accumulate keys mapped to the same clusters.
        for (Map.Entry entry : commands.entrySet()) {
            int clusterId = SpannerUtils.getReplicaClusterID(entry.getKey());
            List<String> list = sMap.get(clusterId);
            if (list == null) {
                list = new ArrayList<>();
                list.add((String) entry.getKey());
                sMap.put(clusterId, list);
            } else {
                list = sMap.get(clusterId);
                list.add((String) entry.getKey());
                sMap.put(clusterId, list);
            }
        }

        // Determine leaders.
        for (Map.Entry entry : sMap.entrySet()) {
            int clusterId = (Integer) entry.getKey();
            for (AddressConfig address : SpannerUtils.getReplicaClusterIPs(clusterId)) {
                try {
                    socket = new Socket(address.host(), address.getClientPort());
                    reader = new Scanner(new InputStreamReader(socket.getInputStream()));
                    if (reader.nextInt() == 1) {

                        // Choose coordinator.
                        if (coordinatorSocket == null) {
                            coordinatorSocket = socket;
                            LogUtils.debug(LOG_TAG, " Coordinator for transaction " + ((WriteCommand)
                                    command)
                                    .getTxn_id() + " is " + coordinatorSocket.getInetAddress().getHostAddress());
                            coordinatorAddress = address;
                        }
                        LogUtils.debug(LOG_TAG, " Leader for Cluster ID " + clusterId + " is " +
                                address.host());
                        sessionMap.put(clusterId, socket);

                        // Leader found.
                        break;
                    } else
                        socket.close();
                } catch (Exception e) {
                    LogUtils.error(LOG_TAG, "Something went wrong", e);
                }
            }
            if (sessionMap.get(clusterId) == null)
                throw new LeaderNotFound(LOG_TAG + " Leader not found for cluster ID " + clusterId);
        }

        if (coordinatorSocket == null || coordinatorAddress == null)
            throw new NoCoordinatorException();

        // Prepare commit object
        for (Map.Entry<Integer, Socket> entry : sessionMap.entrySet())
            updateCommitObjectWithClusterID(entry.getKey(), entry.getValue(), commitObject, sMap, commands);

        // Send commit message to all leaders.
        try {
            for (Map.Entry<Socket, Map<String, String>> entry : commitObject.entrySet()) {
                socket = entry.getKey();
                writer = new ObjectOutputStream(socket.getOutputStream());
                writer.writeObject(new TransportObject(coordinatorAddress, ((WriteCommand) command).getTxn_id(),
                        entry.getValue(), sMap.size(), coordinatorSocket == socket));
                if (socket != coordinatorSocket)
                    socket.close(); // Close connections to all leaders but the coordinator.
            }
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Failed to send commit message(s) to leader(s).", e);
        }

        // Wait for response from coordinator and pass it on to caller.
        try {
            reader = new Scanner(new InputStreamReader(coordinatorSocket.getInputStream()));
            String result = reader.next();
            LogUtils.debug(LOG_TAG, "Message received from coordinator: " + result);
            return result.startsWith("COMMIT");
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Could not get coordinator response.", e);
            return false;
        }
    }

    private void updateCommitObjectWithClusterID(int clusterID, Socket leader, HashMap<Socket, Map<String, String>>
            commitObject, Map<Integer, List<String>> sMap, Map<String, String> commands) {

        Map<String, String> map = new HashMap<>();

        // Create map of key-value pairs for this cluster.
        List<String> keys = sMap.get(clusterID);
        for (String key : keys)
            map.put(key, commands.get(key));

        // Add entry in commit object for the leader of this cluster.
        commitObject.put(leader, map);
    }
}