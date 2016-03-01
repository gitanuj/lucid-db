package com.lucid.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class LogUtils {

    private static final String LOG_PREFIX = "[lucid-db] ";

    private static final Logger LOGGER = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    public static void debug(String tag, String message, Throwable throwable) {
        LOGGER.debug(getLogMessage(tag, message), throwable);
    }

    public static void debug(String tag, String message) {
        LOGGER.debug(getLogMessage(tag, message));
    }

    public static void warn(String tag, String message, Throwable throwable) {
        LOGGER.warn(getLogMessage(tag, message), throwable);
    }

    public static void warn(String tag, String message) {
        LOGGER.warn(getLogMessage(tag, message));
    }

    public static void error(String tag, String message, Throwable throwable) {
        LOGGER.error(getLogMessage(tag, message), throwable);
    }

    public static void error(String tag, String message) {
        LOGGER.error(getLogMessage(tag, message));
    }

    private static String getLogMessage(String tag, String message) {
        return getCaller(tag) + message;
    }

    private static String getCaller(String tag) {
        String caller = "<unknown>";
        StackTraceElement[] trace = new Throwable().fillInStackTrace().getStackTrace();
        if (trace.length < 3) {
            return caller;
        }
        // Walk up the stack looking for the first caller outside of LogUtils.
        // It will be at least 2 frames up, so start there.
        for (int i = 2; i < trace.length; i++) {
            String clazzName = trace[i].getClassName();
            if (!clazzName.contains("Log")) {
                String callingClass = clazzName;
                callingClass = callingClass.substring(callingClass.lastIndexOf('.') + 1);
                callingClass = callingClass.substring(callingClass.lastIndexOf('$') + 1);
                caller = callingClass + "." + trace[i].getMethodName();
                break;
            }
        }
        return String.format(Locale.US, "%s %s: %s ", LOG_PREFIX, tag, caller);
    }
}
