package com.lucid.ycsb;

import com.lucid.common.AddressConfig;
import com.lucid.common.Config;
import com.lucid.common.LogUtils;
import com.lucid.common.Utils;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class YCSBTester {

    private static final String LOG_TAG = "YCSB_TESTER";

    private static final List<Thread> THREADS = new ArrayList<>();

    public static void main(String[] args) {
        Config.init();
//        startTest(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        startUtilsTest();
    }

    private static void startUtilsTest() {
        for (int i = 0; i < Config.SERVER_IPS.size(); ++i) {
            List<AddressConfig> datacenterIPs = Utils.getReplicaIPs(i);
            Utils.printList(datacenterIPs);
        }
    }

    private static void startTest(int numberOfThreads, int protocol) {
        Config.init();

        YCSBDB ycsbdb;
        if (protocol == Config.SPANNER) ycsbdb = new SpannerDB();
        else ycsbdb = new RCDB();

        for (int i = 0; i < numberOfThreads; i++) {
            final int id = i;
            Thread t = new Thread(() -> {
                String table = "table";
                String key = "key" + String.valueOf(id);
                HashMap<String, ByteIterator> values = generateRandomValues();

                Status status = ycsbdb.insert(table, key, values);
                LogUtils.debug(LOG_TAG, id + " write: " + status);
            }, "ycsb-tester-" + id);
            THREADS.add(t);
        }

        THREADS.forEach(Thread::start);

        for (Thread t : THREADS) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LogUtils.error(LOG_TAG, "Something went wrong", e);
            }
        }

        System.exit(0);
    }

    private static HashMap<String, ByteIterator> generateRandomValues() {
        HashMap<String, ByteIterator> values = new HashMap<>();
        values.put("field", new RandomByteIterator(10));
        return values;
    }
}
