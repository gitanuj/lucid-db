package com.lucid.spanner;

import io.atomix.copycat.Command;

import java.util.List;
import java.util.Map;

public class PrepareCommitCommand implements Command<Object> {

    Map<String, String> map;

    public PrepareCommitCommand(Map<String, String> map)
    {
        this.map = map;
    }

    public Object getKeys() {
        return map.keySet();
    }
}
