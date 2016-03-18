package com.lucid.rc;

import com.lucid.common.*;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

public class RCClient implements YCSBClient {

    private static final String LOG_TAG = "RC_CLIENT";

    private class WriteFlags {
        private int writesReturned;
        private int writeThreadsCounter;
        private Object writesWaitOneMe;
        WriteMajoritySelector writeMajoritySelector;
        private boolean writeSuccessful;
        private long writeTxnId;

        WriteFlags() {
            this.writesReturned = 0;
            this.writeThreadsCounter = 0;
            this.writesWaitOneMe = new Object();
            this.writeMajoritySelector = new WriteMajoritySelector();
            this.writeSuccessful = false;
            this.writeTxnId = Config.TXN_ID_NOT_APPLICABLE;
        }

        public WriteMajoritySelector getWriteMajoritySelector() {
            return writeMajoritySelector;
        }

        public Object getWritesWaitOneMe() {
            return writesWaitOneMe;
        }

        public int getWritesReturned() {
            return writesReturned;
        }

        public void setWritesReturned(int writesReturned) {
            this.writesReturned = writesReturned;
        }

        public int getWriteThreadsCounter() {
            return writeThreadsCounter;
        }

        public void setWriteThreadsCounter(int writeThreadsCounter) {
            this.writeThreadsCounter = writeThreadsCounter;
        }

        public boolean isWriteSuccessful() {
            return writeSuccessful;
        }

        public void setWriteSuccessful(boolean writeSuccessful) {
            this.writeSuccessful = writeSuccessful;
        }

        public long getWriteTxnId() {
            return writeTxnId;
        }

        public void setWriteTxnId(long writeTxnId) {
            this.writeTxnId = writeTxnId;
        }
    }

    private class ReadFlags {
        private int readThreadsCounter;
        private int readsReturned;
        private ReadMajoritySelector readMajoritySelector;
        private Object readsWaitOnMe;
        private long highestVersionRead;
        private String highestVersionResult;

        public ReadFlags() {
            this.readsReturned = 0;
            this.readsWaitOnMe = new Object();
            this.readThreadsCounter = 0;
            this.highestVersionRead = -1;
            this.highestVersionResult = null;
            this.readMajoritySelector = new ReadMajoritySelector();
        }

        public Object getReadsWaitOnMe() {
            return readsWaitOnMe;
        }

        public ReadMajoritySelector getReadMajoritySelector() {
            return readMajoritySelector;
        }

        public void setReadMajoritySelector(ReadMajoritySelector readMajoritySelector) {
            this.readMajoritySelector = readMajoritySelector;
        }

        public int getReadThreadsCounter() {
            return readThreadsCounter;
        }

        public void setReadThreadsCounter(int readThreadsCounter) {
            this.readThreadsCounter = readThreadsCounter;
        }

        public int getReadsReturned() {
            return readsReturned;
        }

        public void setReadsReturned(int readsReturned) {
            this.readsReturned = readsReturned;
        }

        public long getHighestVersionRead() {
            return highestVersionRead;
        }

        public void setHighestVersionRead(long highestVersionRead) {
            this.highestVersionRead = highestVersionRead;
        }

        public String getHighestVersionResult() {
            return highestVersionResult;
        }

        public void setHighestVersionResult(String highestVersionResult) {
            this.highestVersionResult = highestVersionResult;
        }

    }

    @Override
    public String executeQuery(Query query) throws Exception {
        LogUtils.debug(LOG_TAG, "Executing query: " + query);

        List<AddressConfig> queryShards = Utils.getReplicaClusterIPs(((ReadQuery) query).key());
        Thread[] clientRequests = new Thread[queryShards.size()];
        ReadFlags readFlags = new ReadFlags();

        // Execute query on all servers.
        for (AddressConfig shard : queryShards) {
            try {
                clientRequests[readFlags.getReadThreadsCounter()] = new Thread(new QueryCluster(shard, (ReadQuery)
                        query, readFlags));
                clientRequests[readFlags.getReadThreadsCounter()].start();
                readFlags.setReadThreadsCounter(readFlags.getReadThreadsCounter() + 1);
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Key: " + ((ReadQuery) query).key() + ". Something went wrong while" +
                        " starting thread number " + (readFlags.getReadThreadsCounter() - 1), e);
            }
        }

        LogUtils.debug(LOG_TAG, "Number of read request-threads made: " + readFlags.getReadThreadsCounter());

        // Sleep till notified either when a majority of requests have come back, or more than 10 seconds have elapsed.
        synchronized (readFlags.getReadsWaitOnMe()) {
            readFlags.getReadsWaitOnMe().wait(10 * 1000);
        }
        // Interrupt all running read threads.
        interruptAllRunningThreads(clientRequests, readFlags.getReadThreadsCounter());

        if (readFlags.getReadsReturned() < readFlags.getReadThreadsCounter() / 2) // Read unsuccessful because majority of read
            // threads
            // did not return.
            throw new UnsuccessfulReadException("Read unsuccessful because majority of threads did not return.");

        return readFlags.getHighestVersionResult();
    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        LogUtils.debug(LOG_TAG, "Executing command: " + command);

        if (command instanceof WriteCommand) {
            try {
                // Select shard of first key in WriteCommand to be the coordinator for this transaction.
                List<AddressConfig> coordinators = Utils.getReplicaClusterIPs(((WriteCommand) command)
                        .getFirstKeyInThisCommand());
                Thread[] clientRequests = new Thread[coordinators.size()];
                WriteFlags writeFlags = new WriteFlags();
                writeFlags.setWriteTxnId(((WriteCommand) command).getTxn_id());

                // Execute query on all servers.
                for (AddressConfig shard : coordinators) {
                    try {
                        clientRequests[writeFlags.getWriteThreadsCounter()] = new Thread(new CommandCluster(shard,
                                (WriteCommand) command, writeFlags));
                        clientRequests[writeFlags.getWriteThreadsCounter()].start();
                        writeFlags.setWriteThreadsCounter(writeFlags.getWriteThreadsCounter() + 1);
                    } catch (Exception e) {
                        LogUtils.debug(LOG_TAG, "Transaction ID: " + writeFlags.getWriteTxnId() + ". Something " +
                                "went wrong " +
                                "while" +
                                " starting thread number " + (writeFlags.getWriteThreadsCounter() - 1), e);
                    }
                }

                LogUtils.debug(LOG_TAG, "Number of write request-threads made: " + writeFlags.getWriteThreadsCounter());

                // Sleep till notified either when a majority of requests have come back, or more than 10 seconds
                // have elapsed.
                synchronized (writeFlags.getWritesWaitOneMe()) {
                    writeFlags.getWritesWaitOneMe().wait(10 * 1000);
                }
                interruptAllRunningThreads(clientRequests, writeFlags.getWriteThreadsCounter());

                if (writeFlags.isWriteSuccessful())
                    return true;

            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong.", e);
            }
        } else
            throw new UnexpectedCommand(LOG_TAG + "Command not an instance of WriteCommand.");

        return false;
    }

    private static void interruptAllRunningThreads(Thread[] threads, int number) {
        try {
            for (int i = 0; i < number; i++)
                if (threads[i].isAlive())
                    threads[i].interrupt();
        } catch (Exception e) {
            LogUtils.debug(LOG_TAG, "Failed to stop threads.", e);
        }
    }

    private class QueryCluster implements Runnable {

        AddressConfig server;
        ReadQuery query;
        Pair<Long, String> result;
        ReadFlags readFlags;

        public QueryCluster(AddressConfig server, ReadQuery query, ReadFlags readFlags) {
            this.server = server;
            this.query = query;
            this.readFlags = readFlags;
        }

        @Override
        public void run() {
            Socket socket = null;
            try {
                // Simulate RC client to datacenter average latency, and write object to datacenter.
                Thread.sleep(Config.RC_CLIENT_TO_DATACENTER_AVG_LATENCY);
                socket = new Socket(server.host(), server.getClientPort());
                ObjectOutputStream writer = new ObjectOutputStream(socket.getOutputStream());
                writer.writeObject(new TransportObject(Config.TXN_ID_NOT_APPLICABLE, query.key()));
                writer.flush();

                // Wait for response.
                ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
                result = (Pair<Long, String>) reader.readObject();

                // Report to ReadMajoritySelector object.
                readFlags.getReadMajoritySelector().threadReturned(result, readFlags);
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Something went wrong.", e);
            } finally {
                Utils.closeQuietly(socket);
            }
        }
    }

    private class ReadMajoritySelector {

        public synchronized void threadReturned(Pair<Long, String> result, ReadFlags readFlags) {
            readFlags.setReadsReturned(readFlags.getReadsReturned() + 1);

            // Update the highest version, if the returned thread has a higher version and the result is not null.
            if (result != null && result.getFirst() > readFlags.getHighestVersionRead()) {
                readFlags.setHighestVersionRead(result.getFirst());
                readFlags.setHighestVersionResult(result.getSecond());
            }

            // If majority threads have returned, notify readsWaitOnMe object.
            if (readFlags.getReadsReturned() > (readFlags.getReadThreadsCounter() / 2))
                synchronized (readFlags.getReadsWaitOnMe()) {
                    readFlags.getReadsWaitOnMe().notify();
                }
        }
    }

    private class CommandCluster implements Runnable {
        AddressConfig server;
        WriteCommand command;
        Pair<Long, String> result;
        WriteFlags writeFlags;

        public CommandCluster(AddressConfig shard, WriteCommand command, WriteFlags writeFlags) {
            this.server = shard;
            this.command = command;
            this.writeFlags = writeFlags;
        }

        @Override
        public void run() {
            Socket socket = null;
            try {
                // Simulate RC client to datacenter average latency, and write object to datacenter.
                Thread.sleep(Config.RC_CLIENT_TO_DATACENTER_AVG_LATENCY);
                socket = new Socket(server.host(), server.getClientPort());
                ObjectOutputStream writer = new ObjectOutputStream(socket.getOutputStream());
                writer.writeObject(new TransportObject(command.getTxn_id(), command.getWriteCommands()));
                writer.flush();

                // Wait for response.
                ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
                result = (Pair<Long, String>) reader.readObject();
                LogUtils.debug(LOG_TAG, "Result for txn id " + writeFlags.getWriteTxnId() + " is " + result);

                // Report to WriteMajoritySelector object.
                writeFlags.getWriteMajoritySelector().threadReturned(result, writeFlags);
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Error in talking to server.", e);
            } finally {
                Utils.closeQuietly(socket);
            }
        }
    }


    private class WriteMajoritySelector {
        private int numberOfCommits;

        WriteMajoritySelector() {
            numberOfCommits = 0;
        }

        public synchronized void threadReturned(Pair<Long, String> result, WriteFlags writeFlags) {
            writeFlags.setWritesReturned(writeFlags.getWritesReturned() + 1);

            if (result.getSecond().startsWith("COMMIT"))
                numberOfCommits++;

            // If majority threads have returned, set result and notify writesWaitOnMe object.
            if (writeFlags.getWritesReturned() > writeFlags.getWriteThreadsCounter() / 2) {
                if (numberOfCommits > (writeFlags.getWriteThreadsCounter() / 2))
                    writeFlags.setWriteSuccessful(true);
                LogUtils.debug(LOG_TAG, "Number of data centres where transaction ID " + writeFlags.getWriteTxnId() +
                        " committed " +
                        "are " + numberOfCommits);

                synchronized (writeFlags.getWritesWaitOneMe()) {
                    writeFlags.getWritesWaitOneMe().notify();
                }
            }
        }
    }
}
