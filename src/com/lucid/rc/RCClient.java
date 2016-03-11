package com.lucid.rc;

import com.lucid.common.LogUtils;
import com.lucid.common.ReadQuery;
import com.lucid.common.Utils;
import com.lucid.common.AddressConfig;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

public class RCClient implements YCSBClient {

    private static final String LOG_TAG = "RC_CLIENT";

    @Override
    public String executeQuery(Query query) throws Exception {

        List<AddressConfig> queryShards = Utils.getReplicaClusterIPs(((ReadQuery)query).key());
        Thread[] clientRequests = new Thread[queryShards.size()];
        Object waitOnMe = new Object();
        int counter = 0;

        // Execute query on all servers, and wait for a response from majority.
        for(AddressConfig shard : queryShards){
            try{
                clientRequests[counter] = new Thread(new QueryCluster(shard, (ReadQuery)query));
                clientRequests[counter].start();
                counter++;
            }
            catch(Exception e){
                LogUtils.debug(LOG_TAG, "Transaction ID: " + ((ReadQuery)query).key() + ". Something went wrong while" +
                        " starting thread number " + (counter - 1),e);
            }
        }

        LogUtils.debug(LOG_TAG, "Number of requests made: " + counter);

        // Sleep till notified when a majority of requests have come back.
        waitOnMe.wait();


    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        return false;
    }

    private class QueryCluster implements Runnable {

        AddressConfig server;
        ReadQuery query;
        ObjectOutputStream writer;

        public QueryCluster(AddressConfig server, ReadQuery query) {
            this.server = server;
            this.query = query;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket(server.host(), server.getClientPort());
                writer = new ObjectOutputStream(socket.getOutputStream());
                writer.write(new TransportObject())
            }
            catch(Exception e){
                LogUtils.debug(LOG_TAG, "Could not connect to server.", e);
            }
        }
    }


//    class CentralPool{
//        table[value][version]
//
//        add enrty to tabel
//    }
}
