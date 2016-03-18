package com.lucid.ycsb;

import com.lucid.common.AddressConfig;
import com.lucid.common.Config;
import com.lucid.common.LogUtils;
import com.lucid.common.Utils;
import com.lucid.rc.RCServer;
import com.lucid.spanner.SpannerServer;
import com.lucid.spanner.SpannerUtils;

import java.util.ArrayList;
import java.util.List;

public class StartDatacenter {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Run as: StartDatacenter <PROROCOL> <CLUSTERID>");
            return;
        }
        int datacenterID = Integer.parseInt(args[1]);

        List<Integer> indexes = Utils.getDatacenterIndexes(datacenterID);

        if ((Integer.parseInt(args[0])) == Config.SPANNER) {
            LogUtils.debug("StartServers", "Starting Spanner cluster");
            for (int index : indexes) {
                final int ind = index;
                Utils.startThreadWithName(() -> {
                    AddressConfig config = Config.SERVER_IPS.get(ind);
                    new SpannerServer(config, ind);
                }, "server-" + ind);
            }
        } else {
            LogUtils.debug("StartServers", "Starting RC cluster");
            for (int index : indexes) {
                final int ind = index;
                Utils.startThreadWithName(() -> {
                    AddressConfig config = Config.SERVER_IPS.get(ind);
                    new RCServer(config, ind);
                }, "server-" + ind);
            }
        }

    }
}
