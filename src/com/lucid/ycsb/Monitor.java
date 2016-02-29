package com.lucid.ycsb;

public class Monitor {

    private final long threadId;

    public Monitor(long threadId) {
        this.threadId = threadId;
    }

    public long getThreadId() {
        return threadId;
    }
}
