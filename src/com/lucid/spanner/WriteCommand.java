package com.lucid.spanner;

import io.atomix.copycat.Command;
import java.util.HashMap;

public class WriteCommand implements Command<Object> {

    private long txn_id;
    private HashMap<String, String> commands;

    public long getTxn_id() {
        return txn_id;
    }

    public void setTxn_id(long txn_id) {
        this.txn_id = txn_id;
    }

    public WriteCommand(HashMap<String, String> map) {
        this.commands = map;
    }

    public HashMap<String, String> getWriteCommands() {
        return this.commands;
    }
}
