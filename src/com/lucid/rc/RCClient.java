package com.lucid.rc;

import com.lucid.common.Utils;
import com.lucid.spanner.AddressConfig;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

public class RCClient implements YCSBClient {

    private static final String LOG_TAG = "RC_CLIENT";

    @Override
    public String executeQuery(Query query) throws Exception {

        // Execute query on all servers, and wait for a response from majority.
        for(AddressConfig shard : Utils.getReplicaClusterIPs(query.))
    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        return false;
    }


    class CentralPool{
        table[value][version]

        add enrty to tabel
    }
}
