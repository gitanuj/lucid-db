package com.lucid.ycsb;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class SpannerDBTester {

    private static final List<Thread> THREADS = new ArrayList<>();

    public static void main(String[] args) {
        testSpannerDb();
    }

    private static void testSpannerDb() {
        SpannerDB spannerDB = new SpannerDB();

        // TODO Fails test for 1000 threads. Investigate!
        for (int i = 0; i < 100; i++) {
            final int id = i;
            Thread t = new Thread(() -> {
                spannerDB.insert("table", String.valueOf(id), generateRandomValues());
                System.out.println(id);
            });
            THREADS.add(t);
        }

        THREADS.forEach(Thread::start);

        for (Thread t : THREADS) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
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
