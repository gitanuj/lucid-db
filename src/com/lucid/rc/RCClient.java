package com.lucid.rc;

import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import com.lucid.common.LogUtils;
import com.lucid.common.Lucid;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.client.CopycatClient;

import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RCClient implements YCSBClient {

    private static final String LOG_TAG = "RC_CLIENT";

    @Override
    public String executeQuery(Query query) throws Exception {

        return null;
    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        return false;
    }
}
