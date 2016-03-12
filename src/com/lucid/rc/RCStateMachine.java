package com.lucid.rc;

import com.lucid.common.LogUtils;
import com.lucid.common.Pair;

import java.util.HashMap;
import java.util.Map;

public class RCStateMachine {

    private static final String LOG_TAG = "RC_STATE_MACHINE";

    private final Map<String, Pair<Long, String>> MAP = new HashMap<>();

    public void write(long txnId, Map<String, String> map) {
        LogUtils.debug(LOG_TAG, "Received write command: " + map);
        for (String key : map.keySet()) {
            if (txnId > getStoredVersion(key)) {
                Pair<Long, String> value = new Pair<>(txnId, map.get(key));
                MAP.put(key, value);
            }
        }
    }

    public Pair<Long, String> read(String key) {
        LogUtils.debug(LOG_TAG, "Received read query for key: " + key);
        return MAP.get(key);
    }

    private long getStoredVersion(String key) {
        Pair<Long, String> pair = MAP.get(key);
        if (pair != null) {
            return pair.getFirst();
        }
        return -1;
    }
}
