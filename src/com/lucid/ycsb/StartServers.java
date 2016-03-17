package com.lucid.ycsb;

import com.lucid.common.AddressConfig;
import com.lucid.common.Config;
import com.lucid.common.Utils;
import com.lucid.rc.RCServer;
import com.lucid.spanner.SpannerServer;

public class StartServers {

    public static void main(String[] args) {

        for (int i = 0; i < Config.SERVER_IPS.size(); i++) {
            final int index = i;
            if((Integer.parseInt(args[0])) != Config.SPANNER)
                Utils.startThreadWithName(() -> {
                    AddressConfig config = Config.SERVER_IPS.get(index);
                    new RCServer(config, index);
                }, "server-" + i);
            else
                Utils.startThreadWithName(() -> {
                    AddressConfig config = Config.SERVER_IPS.get(index);
                    new SpannerServer(config, index);
                }, "server-" + i);

        }
    }
}
