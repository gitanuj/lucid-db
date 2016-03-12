package com.lucid.ycsb;

import com.lucid.common.LogUtils;
import com.lucid.common.WriteCommand;

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

    private static final long BATCH_EXPIRY = 50; //ms

    private static final long REQUEST_TIMEOUT = 15 * 1000; //ms

    private final ScheduledExecutorService batchExpiryExecutor = Executors.newScheduledThreadPool(1);

    private final ScheduledExecutorService timeoutExecutor = Executors.newScheduledThreadPool(1);

    private final ExecutorService executorService = Executors.newFixedThreadPool(MAX_REQUEST_THREADS, new ThreadFactory() {
        private int id;

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r::run, "batcher-" + id++);
        }
    });

    private final List<WriteObject> batch = new ArrayList<>();

    private final Map<Long, Semaphore> waitingLocks = new HashMap<>();

    private final Map<Long, Boolean> resultMap = new HashMap<>();

    private YCSBClient ycsbClient;

    private ScheduledFuture lastBatchExpiryTask;

    private long txnIdSequence = 0;

    private Batcher() {
    }

    public static Batcher getInstance() {
        return INSTANCE;
    }

    public void setYCSBClient(YCSBClient ycsbClient) {
        this.ycsbClient = ycsbClient;
    }

    public boolean write(WriteObject writeObject) {
        try {
            long txnId = addToBatch(writeObject);
            waitToComplete(txnId);
            return checkResult(txnId);
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Something went wrong while batching write", e);
            return false;
        }
    }

    private long addToBatch(WriteObject writeObject) {
        synchronized (batch) {
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

            if (batch.size() < MAX_BATCH_SIZE) {
                lastBatchExpiryTask = batchExpiryExecutor.schedule(task, BATCH_EXPIRY, TimeUnit.MILLISECONDS);
            } else {
                task.run();
            }

            // Create semaphore
            if (waitingLocks.get(txnIdSequence) == null) {
                waitingLocks.put(txnIdSequence, new Semaphore(0));
            }

            batch.add(writeObject);
            return txnIdSequence;
        }
    }

    private void waitToComplete(long txnId) throws InterruptedException {
        waitingLocks.get(txnId).acquire();
    }

    private void notifyCompletion(long txnId, boolean result, int numWaitingThreads) {
        synchronized (resultMap) {
            resultMap.put(txnId, result);
            waitingLocks.remove(txnId).release(numWaitingThreads);
        }
    }

    private boolean checkResult(long txnId) {
        synchronized (resultMap) {
            // TODO When to remove result from map?
            return resultMap.get(txnId);
        }
    }

    private class BatchProcessor implements Runnable {

        private final long txnId;

        private final List<WriteObject> batch;

        public BatchProcessor(long txnId, List<WriteObject> writeObjects) {
            this.txnId = txnId;
            this.batch = writeObjects;
        }

        @Override
        public void run() {
            LogUtils.debug(LOG_TAG, "Starting request for txnId: " + txnId);

            boolean result = false;
            try {
                WriteCommand writeCommand = new WriteCommand(txnId, YCSBUtils.toCommandsMap(batch));
                result = ycsbClient.executeCommand(writeCommand);
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Failed to executeCommand", e);
            }

            try {
                notifyCompletion(txnId, result, batch.size());
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Failed to notify completion", e);
            }
        }
    }
}
