package com.lucid.ycsb;

import com.lucid.common.AddressConfig;
import com.lucid.common.Config;
import com.lucid.common.Utils;
import com.lucid.rc.RCServer;

public class StartServers {

    public static void main(String[] args) {

        for (int i = 0; i < Config.SERVER_IPS.size(); i++) {
            final int index = i;

            Utils.startThreadWithName(() -> {
                AddressConfig config = Config.SERVER_IPS.get(index);
                new RCServer(config, index);
            }, "server-" + i);
        }
    }
}
