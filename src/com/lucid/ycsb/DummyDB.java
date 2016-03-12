package com.lucid.ycsb;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

public class DummyDB extends YCSBDB {

    @Override
    public YCSBClient getYCSBClient() {
        return new DummyClient();
    }

    private class DummyClient implements YCSBClient {

        @Override
        public String executeQuery(Query query) throws Exception {
            return null;
        }

        @Override
        public boolean executeCommand(Command command) throws Exception {
            return true;
        }
    }
}
