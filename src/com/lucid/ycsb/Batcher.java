package com.lucid.ycsb;

import com.lucid.common.LogUtils;
import com.lucid.spanner.SpannerClient;
import com.lucid.spanner.WriteCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Batcher {

    private static final String LOG_TAG = "BATCHER";

    private static final Batcher INSTANCE = new Batcher();

    private static final int MAX_REQUEST_THREADS = 10;

    private static final int MAX_BATCH_SIZE = 5;

    private static final long BATCH_EXPIRY = 100; //ms

    private static final long REQUEST_TIMEOUT = 15 * 1000; //ms

    private final ScheduledExecutorService batchExpiryExecutor = Executors.newScheduledThreadPool(1);

    private final ScheduledExecutorService timeoutExecutor = Executors.newScheduledThreadPool(1);

    private final ExecutorService executorService = Executors.newFixedThreadPool(MAX_REQUEST_THREADS);

    private final List<WriteObject> batch = new ArrayList<>();

    // Txn ID vs List of monitor objects storing thread ID
    private final Map<Long, List<Monitor>> waitingList = new HashMap<>();

    // Thread ID vs Result map
    private final Map<Long, Boolean> resultMap = new HashMap<>();

    private ScheduledFuture lastBatchExpiryTask;

    private long txnIdSequence = 0;

    private Batcher() {
    }

    public static Batcher getInstance() {
        return INSTANCE;
    }

    public boolean write(WriteObject writeObject) {
        long txnId = addToBatch(writeObject);
        try {
            waitToComplete(txnId);
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Something went wrong while waiting", e);
            return false;
        }
        return checkResult();
    }

    private long addToBatch(WriteObject writeObject) {
        synchronized (batch) {
            processBatchIfRequired();
            batch.add(writeObject);
            return txnIdSequence;
        }
    }

    private void processBatchIfRequired() {
        Runnable task = () -> {
            synchronized (batch) {
                BatchProcessor batchProcessor = new BatchProcessor(txnIdSequence, new ArrayList<>(batch));
                batch.clear();
                txnIdSequence++;

                final Future handler = executorService.submit(batchProcessor);
                timeoutExecutor.schedule((Runnable) () -> {
                    try {
                        handler.cancel(true);
                    } catch (Exception e) {
                        LogUtils.error(LOG_TAG, "Failed to cancel BatchProcessor on timeout", e);
                    }
                }, REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            }
        };

        // Remove any expiry tasks
        if (lastBatchExpiryTask != null) {
            lastBatchExpiryTask.cancel(true);
            lastBatchExpiryTask = null;
        }

        synchronized (batch) {
            if (batch.size() < MAX_BATCH_SIZE) {
                lastBatchExpiryTask = batchExpiryExecutor.schedule(task, BATCH_EXPIRY, TimeUnit.MILLISECONDS);
            } else {
                task.run();
            }
        }
    }

    private void waitToComplete(long txnId) throws InterruptedException {
        final Monitor monitor = new Monitor(Thread.currentThread().getId());
        synchronized (waitingList) {
            List<Monitor> monitorList = waitingList.get(txnId);
            if (monitorList == null) {
                monitorList = new ArrayList<>();
                waitingList.put(txnId, monitorList);
            }
            monitorList.add(monitor);
        }

        synchronized (monitor) {
            monitor.wait();
        }
    }

    private void notifyCompletion(long txnId, boolean result) {
        List<Monitor> monitorList;
        synchronized (waitingList) {
            monitorList = waitingList.remove(txnId);
        }

        if (monitorList == null) {
            return;
        }

        synchronized (resultMap) {
            for (Monitor monitor : monitorList) {
                resultMap.put(monitor.getThreadId(), result);
                synchronized (monitor) {
                    monitor.notify();
                }
            }
        }
    }

    private boolean checkResult() {
        synchronized (resultMap) {
            Boolean result = resultMap.remove(Thread.currentThread().getId());
            if (result == null) {
                return false;
            }
            return result;
        }
    }

    private class BatchProcessor implements Runnable {

        private long txnId;

        private List<WriteObject> batch;

        public BatchProcessor(long txnId, List<WriteObject> writeObjects) {
            this.txnId = txnId;
            this.batch = writeObjects;
        }

        @Override
        public void run() {
            boolean result = false;
            try {
                WriteCommand writeCommand = new WriteCommand(txnId, YCSBUtils.toCommandsMap(batch));
                SpannerClient spannerClient = new SpannerClient();
                result = spannerClient.executeCommand(writeCommand);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                notifyCompletion(txnId, result);
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Failed to notify completion", e);
            }
        }
    }
}
