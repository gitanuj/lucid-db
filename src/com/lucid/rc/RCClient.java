package com.lucid.rc;

import com.lucid.common.*;
import com.lucid.ycsb.YCSBClient;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;

public class RCClient implements YCSBClient {

    private static final String LOG_TAG = "RC_CLIENT";
    private ReadMajoritySelector readMajoritySelector;
    private WriteMajoritySelector writeMajoritySelector;
    private Object readsWaitOnMe;
    private Object writesWaitOnMe;
    private int readThreadsCounter;
    private int readsReturned;
    private long highestVersionRead;
    private String highestVersionResult;
    private int writesReturned;
    private int writeThreadsCounter;
    private boolean writeSuccessful;
    private long writeTxnId;


    @Override
    public String executeQuery(Query query) throws Exception {

        List<AddressConfig> queryShards = Utils.getReplicaClusterIPs(((ReadQuery) query).key());
        Thread[] clientRequests = new Thread[queryShards.size()];
        readsWaitOnMe = new Object();
        readThreadsCounter = 0;
        readMajoritySelector = new ReadMajoritySelector();
        readsReturned = 0;
        highestVersionRead = -1;
        highestVersionResult = null;

        // Execute query on all servers.
        for (AddressConfig shard : queryShards) {
            try {
                clientRequests[readThreadsCounter] = new Thread(new QueryCluster(shard, (ReadQuery) query));
                clientRequests[readThreadsCounter].start();
                readThreadsCounter++;
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Key: " + ((ReadQuery) query).key() + ". Something went wrong while" +
                        " starting thread number " + (readThreadsCounter - 1), e);
            }
        }

        LogUtils.debug(LOG_TAG, "Number of read request-threads made: " + readThreadsCounter);

        // Sleep till notified either when a majority of requests have come back, or more than 10 seconds have elapsed.
        synchronized (readsWaitOnMe) {
            readsWaitOnMe.wait(10 * 1000);
        }

        // Interrupt all running read threads.
        interruptAllRunningThreads(clientRequests, readThreadsCounter);

        if (readsReturned < readThreadsCounter / 2) // Read unsuccessful because majority of read threads
            // did not return.
            throw new UnsuccessfulReadException("Read unsuccessful because majority of threads did not return.");

        return highestVersionResult;
    }

    @Override
    public boolean executeCommand(Command command) throws Exception {
        if (command instanceof WriteCommand) {
            try {
                // Select shard of first key in WriteCommand to be the coordinator for this transaction.
                List<AddressConfig> coordinators = Utils.getReplicaClusterIPs(((WriteCommand) command)
                        .getFirstKeyInThisCommand());
                Thread[] clientRequests = new Thread[coordinators.size()];
                writesWaitOnMe = new Object();
                writesReturned = 0;
                writeThreadsCounter = 0;
                writeMajoritySelector = new WriteMajoritySelector();
                writeSuccessful = false;
                writeTxnId = ((WriteCommand) command).getTxn_id();

                // Execute query on all servers.
                for (AddressConfig shard : coordinators) {
                    try {
                        clientRequests[writeThreadsCounter] = new Thread(new CommandCluster(shard, (WriteCommand) command));
                        clientRequests[writeThreadsCounter].start();
                        writeThreadsCounter++;
                    } catch (Exception e) {
                        LogUtils.debug(LOG_TAG, "Transaction ID: " + writeTxnId + ". Something " +
                                "went wrong " +
                                "while" +
                                " starting thread number " + (writeThreadsCounter - 1), e);
                    }
                }

                LogUtils.debug(LOG_TAG, "Number of write request-threads made: " + writeThreadsCounter);

                // Sleep till notified either when a majority of requests have come back, or more than 10 seconds have elapsed.
                synchronized (writesWaitOnMe) {
                    writesWaitOnMe.wait(10 * 1000);
                }

                // Interrupt all running read threads.
                interruptAllRunningThreads(clientRequests, writeThreadsCounter);

                if (writeSuccessful)
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
        ObjectOutputStream writer;
        ObjectInputStream reader;
        Pair<Long, String> result;

        public QueryCluster(AddressConfig server, ReadQuery query) {
            this.server = server;
            this.query = query;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket(server.host(), server.getClientPort());
                writer = new ObjectOutputStream(socket.getOutputStream());
                writer.writeObject(new TransportObject(Config.TXN_ID_NOT_APPLICABLE, query.key()));

                // Wait for response.
                reader = new ObjectInputStream(socket.getInputStream());
                result = (Pair<Long, String>) reader.readObject();

                socket.close();

                // Report to ReadMajoritySelector object.
                readMajoritySelector.threadReturned(result.getSecond(), result.getFirst());
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Error in talking to server.", e);
            }
        }
    }

    private class ReadMajoritySelector {

        public synchronized void threadReturned(String value, long version) {
            readsReturned++;

            // Update the highest version, if the returned thread has a higher version.
            if (version > highestVersionRead) {
                highestVersionRead = version;
                highestVersionResult = value;
            }

            // If majority threads have returned, notify readsWaitOnMe object.
            if (readsReturned > ((readThreadsCounter / 2) + 1))
                synchronized (readsWaitOnMe){
                    readsWaitOnMe.notify();
                }
        }
    }

    private class CommandCluster implements Runnable {
        AddressConfig server;
        WriteCommand command;
        ObjectOutputStream writer;
        Scanner reader;
        String result;

        public CommandCluster(AddressConfig shard, WriteCommand command) {
            this.server = shard;
            this.command = command;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket(server.host(), server.getClientPort());
                writer = new ObjectOutputStream(socket.getOutputStream());
                writer.writeObject(new TransportObject(command.getTxn_id(), command.getWriteCommands()));

                // Wait for response.
                reader = new Scanner(socket.getInputStream());
                result = reader.next();
                LogUtils.debug(LOG_TAG, "Result for txn id " + writeTxnId + " is " + result);
                socket.close();

                // Report to WriteMajoritySelector object.
                writeMajoritySelector.threadReturned(result);
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Error in talking to server.", e);
            }
        }
    }


    private class WriteMajoritySelector {
        private int numberOfCommits;

        WriteMajoritySelector() {
            numberOfCommits = 0;
        }

        public synchronized void threadReturned(String result) {
            writesReturned++;

            if (result.startsWith("COMMIT"))
                numberOfCommits++;

            // If majority threads have returned, set result and notify writesWaitOnMe object.
            if (writesReturned > writeThreadsCounter / 2) {
                if (numberOfCommits > ((writeThreadsCounter / 2) + 1))
                    writeSuccessful = true;
                LogUtils.debug(LOG_TAG, "Number of data centres where transaction ID " + writeTxnId + " committed " +
                        "are " + numberOfCommits);

                synchronized (writesWaitOnMe){
                    writesWaitOnMe.notify();
                }
            }
        }
    }
}
