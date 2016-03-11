package com.lucid.common;

import ch.qos.logback.classic.Level;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class LogUtils {

    private static final String LOG_PREFIX = "[lucid-db] ";

    private static final LogLevel LOG_LEVEL = LogLevel.DEBUG;

    private static final boolean ENABLE_COPYCAT_DEBUG_LOGS = false;

    private static final Logger LOGGER = new PrintLogger();

    private enum LogLevel {
        ERROR, WARN, DEBUG
    }

    static {
        if (ENABLE_COPYCAT_DEBUG_LOGS) {
            setSL4JLoggingLevel(Level.DEBUG);
        } else {
            setSL4JLoggingLevel(Level.ERROR);
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

    private static void setSL4JLoggingLevel(ch.qos.logback.classic.Level level) {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(level);
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

    private static class PrintLogger implements Logger {

        @Override
        public void debug(String message, Throwable throwable) {
            if (LOG_LEVEL.ordinal() >= LogLevel.DEBUG.ordinal()) {
                print(message, throwable);
            }
        }

        @Override
        public void debug(String message) {
            if (LOG_LEVEL.ordinal() >= LogLevel.DEBUG.ordinal()) {
                print(message, null);
            }
        }

        @Override
        public void warn(String message, Throwable throwable) {
            if (LOG_LEVEL.ordinal() >= LogLevel.WARN.ordinal()) {
                print(message, throwable);
            }
        }

        @Override
        public void warn(String message) {
            if (LOG_LEVEL.ordinal() >= LogLevel.WARN.ordinal()) {
                print(message, null);
            }
        }

        @Override
        public void error(String message, Throwable throwable) {
            if (LOG_LEVEL.ordinal() >= LogLevel.ERROR.ordinal()) {
                print(message, throwable);
            }
        }

        @Override
        public void error(String message) {
            if (LOG_LEVEL.ordinal() >= LogLevel.ERROR.ordinal()) {
                print(message, null);
            }
        }

        private void print(String message, Throwable throwable) {
            if (message != null) {
                System.out.println(message);
            }
            if (throwable != null) {
                throwable.printStackTrace();
            }
        }
    }
}
