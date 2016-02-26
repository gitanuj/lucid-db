package com.lucid.spanner;

import io.atomix.copycat.Command;

public class PrepareCommitCommand implements Command<Object> {

    private Object keys;

    public PrepareCommitCommand(Object keys) {
        this.keys = keys;
    }

    public Object getKeys() {
        return keys;
    }
}
