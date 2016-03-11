package com.lucid.common;

import io.atomix.copycat.Command;

public class CommitCommand implements Command<Object> {

    private Object data;

    public CommitCommand(Object data) {
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
