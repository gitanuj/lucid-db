package com.lucid.common;

import ch.qos.logback.classic.Level;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class LogUtils {

    private static final String LOG_PREFIX = "[lucid-db] ";

    private static LogLevel COPYCAT_LOG_LEVEL;

    private static LogLevel LUCID_LOG_LEVEL;

    private static Logger LOGGER;

    public enum LogLevel {
        NONE(0), ERROR(1), WARN(2), DEBUG(3);

        private static Map<Integer, LogLevel> MAP = new HashMap<>();

        static {
            for (LogLevel logLevel : LogLevel.values()) {
                MAP.put(logLevel.getId(), logLevel);
            }
        }

        private int id;

        LogLevel(int id) {
            this.id = id;
        }

        public int getId() {
            return this.id;
        }

        public static LogLevel getLogLevelById(int id) {
            return MAP.get(id);
        }
    }

    private interface Logger {

        void debug(String message, Throwable throwable);

        void debug(String message);

        void warn(String message, Throwable throwable);

        void warn(String message);

        void error(String message, Throwable throwable);

        void error(String message);
    }

    public static void init() {
    }

    public static void setCopycatLogLevel(LogLevel logLevel) {
        COPYCAT_LOG_LEVEL = logLevel;

        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        switch (logLevel) {
            case NONE:
                root.setLevel(Level.OFF);
                break;
            case ERROR:
                root.setLevel(Level.ERROR);
                break;
            case WARN:
                root.setLevel(Level.WARN);
                break;
            case DEBUG:
                root.setLevel(Level.DEBUG);
                break;
        }
    }

    public static void setLucidLogLevel(LogLevel logLevel) {
        LUCID_LOG_LEVEL = logLevel;

        if (logLevel == LogLevel.NONE) {
            LOGGER = new NoOpLogger();
        } else {
            LOGGER = new PrintLogger();
        }
    }

    public static void debug(String tag, String message, Throwable throwable) {
        LOGGER.debug(getLogMessage(tag, message), throwable);
    }

    public static void debug(String tag, String message) {
        LOGGER.warn(getLogMessage(tag, message));
    }

    public static void warn(String tag, String message, Throwable throwable) {
        LOGGER.debug(getLogMessage(tag, message), throwable);
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

    private static class SL4JLogger implements Logger {

        private org.slf4j.Logger LOGGER = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);

        @Override
        public void debug(String message, Throwable throwable) {
            LOGGER.debug(message, throwable);
        }

        @Override
        public void debug(String message) {
            LOGGER.debug(message);
        }

        @Override
        public void warn(String message, Throwable throwable) {
            LOGGER.warn(message, throwable);
        }

        @Override
        public void warn(String message) {
            LOGGER.warn(message);
        }

        @Override
        public void error(String message, Throwable throwable) {
            LOGGER.error(message, throwable);
        }

        @Override
        public void error(String message) {
            LOGGER.error(message);
        }
    }

    private static class NoOpLogger implements Logger {

        @Override
        public void debug(String message, Throwable throwable) {

        }

        @Override
        public void debug(String message) {

        }

        @Override
        public void warn(String message, Throwable throwable) {

        }

        @Override
        public void warn(String message) {

        }

        @Override
        public void error(String message, Throwable throwable) {

        }

        @Override
        public void error(String message) {

        }
    }

    private static class PrintLogger implements Logger {

        @Override
        public void debug(String message, Throwable throwable) {
            if (LUCID_LOG_LEVEL.ordinal() >= LogLevel.DEBUG.ordinal()) {
                print(message, throwable);
            }
        }

        @Override
        public void debug(String message) {
            if (LUCID_LOG_LEVEL.ordinal() >= LogLevel.DEBUG.ordinal()) {
                print(message, null);
            }
        }

        @Override
        public void warn(String message, Throwable throwable) {
            if (LUCID_LOG_LEVEL.ordinal() >= LogLevel.WARN.ordinal()) {
                print(message, throwable);
            }
        }

        @Override
        public void warn(String message) {
            if (LUCID_LOG_LEVEL.ordinal() >= LogLevel.WARN.ordinal()) {
                print(message, null);
            }
        }

        @Override
        public void error(String message, Throwable throwable) {
            if (LUCID_LOG_LEVEL.ordinal() >= LogLevel.ERROR.ordinal()) {
                print(message, throwable);
            }
        }

        @Override
        public void error(String message) {
            if (LUCID_LOG_LEVEL.ordinal() >= LogLevel.ERROR.ordinal()) {
                print(message, null);
            }
        }

        synchronized private void print(String message, Throwable throwable) {
            if (message != null) {
                System.out.println(message);
            }
            if (throwable != null) {
                throwable.printStackTrace();
            }
        }
    }
}
