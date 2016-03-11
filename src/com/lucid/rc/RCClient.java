package com.lucid.rc;

import com.lucid.common.LogUtils;
import com.lucid.common.ReadQuery;
import com.lucid.common.Utils;
import com.lucid.common.AddressConfig;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

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
                clientRequests[counter] = new Thread(new QueryCluster(shard, query)).start();
            }
            catch(Exception e){
                LogUtils.debug(LOG_TAG, "Transaction ID: " + ((ReadQuery)query).key() + ". Something went wrong while" +
                        " starting thread number " + (counter - 1),e);
            }
        }
    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        return false;
    }

    private class QueryCluster implements Runnable {

        AddressConfig server;
        ReadQuery query;
        public QueryCluster(AddressConfig server, ReadQuery query) {
            this.server = server;
            this.query = query;
        }

        @Override
        public void run() {

            // Open a connection to the server.
            Socket socket = new Socket(server.host(), server.getClientPort());
        }
    }


//    class CentralPool{
//        table[value][version]
//
//        add enrty to tabel
//    }
}
