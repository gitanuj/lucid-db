package com.lucid.spanner;

import io.atomix.copycat.Query;

public class ReadQuery implements Query<Object> {

    private final Object key;

    public ReadQuery(Object key) {
        this.key = key;
    }

    public Object key() {
        return key;
    }
}
