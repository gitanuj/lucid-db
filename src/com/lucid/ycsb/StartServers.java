package com.lucid.ycsb;

import com.lucid.spanner.AddressConfig;
import com.lucid.spanner.Config;
import com.lucid.spanner.SpannerServer;
import com.lucid.spanner.SpannerUtils;

public class StartServers {

    public static void main(String[] args) {

        for (int i = 0; i < Config.SERVER_IPS.size(); i++) {
            final int index = i;

            SpannerUtils.startThreadWithName(() -> {
                AddressConfig config = Config.SERVER_IPS.get(index);
                new SpannerServer(config, index);
            }, "server-" + i);
        }
    }
}
