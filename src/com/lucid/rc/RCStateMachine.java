package com.lucid.rc;

import com.lucid.common.LogUtils;

import java.util.HashMap;
import java.util.Map;

public class RCStateMachine {

    private static final String LOG_TAG = "RC_STATE_MACHINE";

    private final Map<String, String> MAP = new HashMap<>();

    public void write(Map<String, String> map) {
        LogUtils.debug(LOG_TAG, "Received write command: " + map);
        for (String key : map.keySet()) {
            MAP.put(key, map.get(key));
        }
    }

    public String read(String key) {
        LogUtils.debug(LOG_TAG, "Received read query for key: " + key);
        return MAP.get(key);
    }
}
