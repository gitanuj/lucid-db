package com.lucid.ycsb;

import com.lucid.spanner.SpannerClient;

public class SpannerDB extends YCSBDB {

    @Override
    public YCSBClient getYCSBClient() {
        return new SpannerClient();
    }
}
