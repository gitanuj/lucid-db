package com.lucid.spanner;

import io.atomix.copycat.Command;
import java.util.HashMap;

public class WriteCommand implements Command<Object> {

    private HashMap<String, Object> commands;

    public WriteCommand(HashMap<String, Object> map) {
        this.commands = map;
    }

    public Object getWriteCommand() {
        return this.commands;
    }
}
