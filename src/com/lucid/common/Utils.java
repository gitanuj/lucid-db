package com.lucid.common;

import java.io.Closeable;

/**
 * Created by tanuj on 3/1/16.
 */
public class Utils {

    private static final String LOG_TAG = "UTILS";

    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong while closing stream", e);
            }
        }
    }
}
