package com.lucid.rc;

import com.lucid.common.*;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class RCClient implements YCSBClient {

    private static final String LOG_TAG = "RC_CLIENT";
    private ReadMajoritySelector readMajoritySelector;
    private Object readsWaitOnMe;
    private int readThreadsCounter;
    private int readsSuccessfullyReturned;

    @Override
    public String executeQuery(Query query) throws Exception {

        List<AddressConfig> queryShards = Utils.getReplicaClusterIPs(((ReadQuery)query).key());
        Thread[] clientRequests = new Thread[queryShards.size()];
        readsWaitOnMe = new Object();
        readThreadsCounter = 0;
        readMajoritySelector = new ReadMajoritySelector();
        readsSuccessfullyReturned = 0;

        // Execute query on all servers, and wait for a response from majority.
        for(AddressConfig shard : queryShards){
            try{
                clientRequests[readThreadsCounter] = new Thread(new QueryCluster(shard, (ReadQuery)query));
                clientRequests[readThreadsCounter].start();
                readThreadsCounter++;
            }
            catch(Exception e){
                LogUtils.debug(LOG_TAG, "Transaction ID: " + ((ReadQuery)query).key() + ". Something went wrong while" +
                        " starting thread number " + (readThreadsCounter - 1),e);
            }
        }

        LogUtils.debug(LOG_TAG, "Number of requests made: " + readThreadsCounter);

        // Sleep till notified when a majority of requests have come back, or more than 10 seconds have elapsed.
        readsWaitOnMe.wait(10 * 1000);

        // Interrupt all running read threads.
        for(int i = 0; i < readThreadsCounter; i++)
            if(clientRequests[i].isAlive())
                clientRequests[i].interrupt();

        if(readsSuccessfullyReturned < readThreadsCounter / 2) // Read unsuccessful because majority of read threads
            // did not return.
            throw new UnsuccessfulReadException("Read unsuccessful because majority of threads did not return.");

        return latestVersionRead();
    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        return false;
    }

    private String latestVersionRead(){
        long highestVersion = -1;
        String answer = null;
        for(Map.Entry<String, Long> map : readMajoritySelector.getMap().entrySet()){
            if(map.getValue() > highestVersion) {
                answer = map.getKey();
                highestVersion = map.getValue();
            }
        }
        return answer;
    }

    private class QueryCluster implements Runnable {

        AddressConfig server;
        ReadQuery query;
        ObjectOutputStream writer;
        Scanner reader;
        String[] result;

        public QueryCluster(AddressConfig server, ReadQuery query) {
            this.server = server;
            this.query = query;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket(server.host(), server.getClientPort());
                writer = new ObjectOutputStream(socket.getOutputStream());
                writer.writeObject(new TransportObject(Config.TXN_ID_NOT_APPLICABLE, query.key()));

                // Wait for response.
                reader = new Scanner(socket.getInputStream());
                result = reader.next().split(Config.DELIMITER);

                // Report to ReadMajoritySelector object.
                readMajoritySelector.threadReturned(result[0], Long.parseLong(result[1]));
            }
            catch(Exception e){
                LogUtils.debug(LOG_TAG, "Error in talking to server.", e);
            }
        }
    }

    private class ReadMajoritySelector {

        private HashMap<String, Long> map;

        ReadMajoritySelector(){
            map = new HashMap<>();
        }

        public HashMap<String, Long> getMap() {
            return map;
        }

        public synchronized void threadReturned(String value, long version){
            map.put(value, version);
            readsSuccessfullyReturned++;

            // If majority threads have returned, notify readsWaitOnMe object.
            if(readsSuccessfullyReturned > readThreadsCounter / 2)
                readsWaitOnMe.notify();
        }
    }
}
