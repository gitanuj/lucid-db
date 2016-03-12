package com.lucid.common;

public class Lucid {

    private static final String LOG_TAG = "LUCID";

    private static final Lucid INSTANCE = new Lucid();

    private Lucid() {
    }

    public static final Lucid getInstance() {
        return INSTANCE;
    }

    // Need this to init logging config
    private void init() {
        LogUtils.init();
    }

    public void onClientStarted() {
        init();
    }

    public void onServerStarted() {
        init();
    }
}
