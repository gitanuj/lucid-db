package com.lucid.ycsb;

import com.lucid.rc.RCClient;

public class RCDB extends YCSBDB {

    @Override
    public YCSBClient getYCSBClient() {
        return new RCClient();
    }
}
