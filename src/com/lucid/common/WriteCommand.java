package com.lucid.common;

import io.atomix.copycat.Command;

import java.util.Map;

public class WriteCommand implements Command<Object> {

    private long txn_id;
    private Map<String, String> commands;

    public long getTxn_id() {
        return txn_id;
    }

    public WriteCommand(long txn_id, Map<String, String> map) {
        this.txn_id = txn_id;
        this.commands = map;
    }

    public Map<String, String> getWriteCommands() {
        return this.commands;
    }

    public String getFirstKeyInThisCommand() {
        return commands.entrySet().iterator().next().getKey();
    }

    @Override
    public String toString() {
        return "WriteCommand{" +
                "txn_id=" + txn_id +
                ", commands=" + commands +
                '}';
    }
}
