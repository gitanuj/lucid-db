package com.lucid.spanner;

import io.atomix.copycat.Command;

public class WriteCommand implements Command<Object> {

    private Object data;

    public WriteCommand(Object data) {
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
