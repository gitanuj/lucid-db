package com.lucid.common;

import io.atomix.copycat.Command;

public class AbortCommand implements Command<Object> {

    private Object data;

    public AbortCommand(Object data) {
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
