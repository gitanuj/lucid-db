package com.lucid.ycsb;

import com.lucid.common.LogUtils;
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
        startTest();
    }

    private static void startTest() {

        YCSBDB ycsbdb = new RCDB();

        for (int i = 0; i < 1; i++) {
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
