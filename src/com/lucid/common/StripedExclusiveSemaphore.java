package com.lucid.common;

import java.util.concurrent.Semaphore;

public class StripedExclusiveSemaphore {

    private final int stripes;

    private final Semaphore[] semaphores;

    public StripedExclusiveSemaphore(int stripes) {
        this.stripes = stripes;
        this.semaphores = new Semaphore[stripes];
        for (int i = 0; i < stripes; ++i) {
            semaphores[i] = new Semaphore(1);
        }
    }

    public Semaphore get(String key) {
        return semaphores[getIndex(key)];
    }

    private int getIndex(String key) {
        return Math.abs(key.hashCode()) % stripes;
    }
}
