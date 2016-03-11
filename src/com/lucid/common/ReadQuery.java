package com.lucid.common;

import io.atomix.copycat.Query;

public class ReadQuery implements Query<String> {

    private final String key;

    public ReadQuery(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
