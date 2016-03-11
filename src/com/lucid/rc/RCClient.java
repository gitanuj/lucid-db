package com.lucid.rc;

import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

public class RCClient implements YCSBClient {

    @Override
    public String executeQuery(Query query) throws Exception {
        return null;
    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        return false;
    }
}
