package com.lucid.spanner;

import io.atomix.copycat.Command;

import java.util.List;

public class PrepareCommitCommand implements Command<Object> {

    private List<String> keys;
    private List<Object> values;

    public PrepareCommitCommand(List<String> keys, List<Object> values)
    {
        this.keys = keys;
        this.values = values;
    }

    public Object getKeys() {
        return keys;
    }
}
