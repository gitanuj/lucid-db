package com.lucid.ycsb;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

public interface YCSBClient {

    String executeQuery(Query query) throws Exception;

    boolean executeCommand(Command command) throws Exception;
}
