package com.lucid.rc;

import com.google.common.util.concurrent.Striped;
import com.lucid.common.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class RCServer {

    private static final long RETRY_BACKOFF = 500; //ms

    private final String LOG_TAG;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private final AddressConfig addressConfig;

    private final int clientPort;

    private final int serverPort;

    private final RCStateMachine stateMachine;

    private final List<AddressConfig> datacenterIPs;

    private final List<AddressConfig> replicasIPs;

    private final Map<AddressConfig, ObjectOutputStream> datacenterOutputStreams;

    private final Map<AddressConfig, ObjectOutputStream> replicaOutputStreams;

    private final Map<Long, List<Lock>> txnLocks;

    private final Map<Long, Semaphore> ackLocks;

    private final Map<Long, AtomicInteger> counter;

    private final Striped<Lock> stripedLocks = Striped.lazyWeakLock(100);

    public RCServer(AddressConfig addressConfig, int index) {
        Lucid.getInstance().onServerStarted();
        LOG_TAG = "RC_SERVER-" + index;

        LogUtils.debug(LOG_TAG, "Initializing " + addressConfig);

        this.addressConfig = addressConfig;
        this.clientPort = addressConfig.getClientPort();
        this.serverPort = addressConfig.getServerPort();
        this.stateMachine = new RCStateMachine();
        this.datacenterOutputStreams = new HashMap<>();
        this.replicaOutputStreams = new HashMap<>();
        this.datacenterIPs = Utils.getDatacenterIPs(index);
        this.replicasIPs = Utils.getReplicaClusterIPs(index % Config.NUM_CLUSTERS);
        this.txnLocks = new HashMap<>();
        this.ackLocks = new HashMap<>();
        this.counter = new HashMap<>();

        openServerPort();
        openClientPort();

        connectToDatacenter();
        connectToReplicas();
    }

    private void onDatacenterSocketReady(AddressConfig addressConfig, ObjectOutputStream oos) {
        datacenterOutputStreams.put(addressConfig, oos);

        if (datacenterOutputStreams.size() == datacenterIPs.size()) {
            LogUtils.debug(LOG_TAG, "Fully connected to datacenter");
        }
    }

    private void connectToDatacenter() {
        for (AddressConfig ac : datacenterIPs) {
            executorService.submit(new ConnectToDatacenterRunnable(ac));
        }
    }

    private void onReplicaSocketReady(AddressConfig addressConfig, ObjectOutputStream oos) {
        replicaOutputStreams.put(addressConfig, oos);

        if (replicaOutputStreams.size() == replicasIPs.size()) {
            LogUtils.debug(LOG_TAG, "Fully connected to replicas");
        }
    }

    private void connectToReplicas() {
        for (AddressConfig ac : replicasIPs) {
            executorService.submit(new ConnectToReplicaRunnable(ac));
        }
    }

    private void openServerPort() {
        Runnable runnable = () -> {
            try {
                ServerSocket ss = new ServerSocket(serverPort);
                while (true) {
                    handleServerMsg(ss.accept());
                }
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in accept-server thread", e);
            }
        };

        Utils.startThreadWithName(runnable, "accept-server");
    }

    private void handleServerMsg(Socket server) {
        Runnable runnable = () -> {
            ObjectInputStream inputStream = null;

            try {
                inputStream = new ObjectInputStream(server.getInputStream());
                ServerMsg msg = (ServerMsg) inputStream.readObject();
                handleServerMsg(msg);
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in handle-server thread", e);
            } finally {
                Utils.closeQuietly(inputStream);
            }
        };

        Utils.startThreadWithName(runnable, "handle-server");
    }

    private void handleServerMsg(ServerMsg msg) throws Exception {
        switch (msg.getMessage()) {
            case _2PC_PREPARE:
                handle2PCPrepare(msg);
                break;
            case ACK_2PC_PREPARE:
                handleAck2PCPrepare(msg);
                break;
            case _2PC_ACCEPT:
                handle2PCAccept(msg);
                break;
            case _2PC_COMMIT:
                handle2PCCommit(msg);
                break;
        }
    }

    private void acquireTxnLocks(ServerMsg msg) throws Exception {
        List<Lock> lockList = new ArrayList<>();
        txnLocks.put(msg.getTxn_id(), lockList);
        for (String key : msg.getMap().keySet()) {
            Lock lock = stripedLocks.get(key);
            lock.lock();
            lockList.add(lock);
        }
    }

    private void releaseTxnLocks(ServerMsg msg) throws Exception {
        for (Lock lock : txnLocks.remove(msg.getTxn_id())) {
            lock.unlock();
        }
    }

    private void handle2PCPrepare(ServerMsg msg) throws Exception {
        // Take locks
        acquireTxnLocks(msg);

        // Send 2PC ack to coordinator
        AddressConfig coordinator = msg.getCoordinator();
        ObjectOutputStream oos = datacenterOutputStreams.get(coordinator);
        ServerMsg ackMsg = new ServerMsg(Message.ACK_2PC_PREPARE, msg);
        oos.writeObject(ackMsg);
    }

    private void handleAck2PCPrepare(ServerMsg msg) throws Exception {
        // Let the coordinator know that ack is received
        ackLocks.get(msg.getTxn_id()).release();
    }

    private void handle2PCAccept(ServerMsg msg) throws Exception {
        // Create counter
        AtomicInteger atomicCounter;
        synchronized (counter) {
            atomicCounter = counter.get(msg.getTxn_id());
            if (atomicCounter == null) {
                atomicCounter = new AtomicInteger();
                counter.put(msg.getTxn_id(), atomicCounter);
            }
        }

        int total = replicaOutputStreams.size();
        int count = atomicCounter.incrementAndGet();
        if (count == total / 2 + 1) {
            // Majority reached
            ServerMsg commitMsg = new ServerMsg(Message._2PC_COMMIT, msg);

            // Send 2PC Commit to datacenter servers
            for (ObjectOutputStream oos : datacenterOutputStreams.values()) {
                oos.writeObject(commitMsg);
            }
        }

        // Remove counter
        if (count == total) {
            synchronized (counter) {
                counter.remove(msg.getTxn_id());
            }
        }
    }

    private void handle2PCCommit(ServerMsg msg) throws Exception {
        // Commit values
        stateMachine.write(msg.getTxn_id(), msg.getMap());

        // Release txn locks
        releaseTxnLocks(msg);
    }

    private void openClientPort() {
        Runnable runnable = () -> {
            try {
                ServerSocket ss = new ServerSocket(clientPort);
                while (true) {
                    handleClientMsg(ss.accept());
                }
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in accept-client thread", e);
            }
        };

        Utils.startThreadWithName(runnable, "accept-client");
    }

    private void handleClientMsg(Socket client) {
        Runnable runnable = () -> {
            LogUtils.debug(LOG_TAG, "Handling client msg");

            ObjectInputStream inputStream = null;
            ObjectOutputStream outputStream = null;
            TransportObject msg = null;

            try {
                inputStream = new ObjectInputStream(client.getInputStream());
                outputStream = new ObjectOutputStream(client.getOutputStream());
                msg = (TransportObject) inputStream.readObject();

                // Create semaphore
                ackLocks.put(msg.getTxn_id(), new Semaphore(0));

                handleClientMsg(msg, outputStream);
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in handle-client thread", e);
            } finally {
                Utils.closeQuietly(inputStream);
                Utils.closeQuietly(outputStream);

                // Clear semaphore
                if (msg != null) {
                    ackLocks.remove(msg.getTxn_id());
                }
            }
        };

        Utils.startThreadWithName(runnable, "handle-client");
    }

    private void handleClientMsg(TransportObject msg, ObjectOutputStream outputStream) throws Exception {
        if (msg.getKey() != null) {
            // Read query
            Pair<Long, String> value = stateMachine.read(msg.getKey());
            outputStream.writeObject(value);
        } else {
            // Write command
            ServerMsg serverMsg = new ServerMsg(null, msg.getKey(), msg.getWriteMap(), msg.getTxn_id(), addressConfig);

            // Send 2PC PREPARE to all shards within datacenter
            ServerMsg _2PCPrepare = new ServerMsg(Message._2PC_PREPARE, serverMsg);
            for (ObjectOutputStream oos : datacenterOutputStreams.values()) {
                oos.writeObject(_2PCPrepare);
            }

            // Wait for ack 2PC from all datacenter servers
            ackLocks.get(msg.getTxn_id()).acquire(datacenterOutputStreams.size());

            // Inform replicas and client
            outputStream.writeBytes("COMMIT:" + msg.getTxn_id());
            ServerMsg ack2PCPrepare = new ServerMsg(Message.ACK_2PC_PREPARE, serverMsg);
            for (ObjectOutputStream oos : replicaOutputStreams.values()) {
                oos.writeObject(ack2PCPrepare);
            }
        }
    }

    private Map<String, String> createShardMap(Map<String, String> map) {
        // TODO
        return null;
    }

    private class ConnectToDatacenterRunnable implements Runnable {

        private final AddressConfig addressConfig;

        public ConnectToDatacenterRunnable(AddressConfig addressConfig) {
            this.addressConfig = addressConfig;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket(addressConfig.host(), addressConfig.getServerPort());
                onDatacenterSocketReady(addressConfig, new ObjectOutputStream(socket.getOutputStream()));
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Retrying connection to datacenter server " + addressConfig);
                executorService.schedule(this, RETRY_BACKOFF, TimeUnit.MILLISECONDS);
            }
        }
    }

    private class ConnectToReplicaRunnable implements Runnable {

        private final AddressConfig addressConfig;

        public ConnectToReplicaRunnable(AddressConfig addressConfig) {
            this.addressConfig = addressConfig;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket(addressConfig.host(), addressConfig.getServerPort());
                onReplicaSocketReady(addressConfig, new ObjectOutputStream(socket.getOutputStream()));
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Retrying connection to replica server " + addressConfig);
                executorService.schedule(this, RETRY_BACKOFF, TimeUnit.MILLISECONDS);
            }
        }
    }

    private enum Message {
        _2PC_PREPARE, ACK_2PC_PREPARE, _2PC_ACCEPT, _2PC_COMMIT
    }

    private class ServerMsg implements Serializable {

        private long txn_id;

        private Message message;

        private String key;

        private Map<String, String> map;

        private AddressConfig coordinator;

        public ServerMsg(Message message, ServerMsg serverMsg) {
            this(message, serverMsg.getKey(), serverMsg.getMap(), serverMsg.getTxn_id(), serverMsg.getCoordinator());
        }

        public ServerMsg(Message message, String key, Map<String, String> map, long txn_id, AddressConfig coordinator) {
            this.message = message;
            this.key = key;
            this.map = map;
            this.txn_id = txn_id;
            this.coordinator = coordinator;
        }

        public long getTxn_id() {
            return txn_id;
        }

        public Message getMessage() {
            return message;
        }

        public String getKey() {
            return key;
        }

        public Map<String, String> getMap() {
            return map;
        }

        public AddressConfig getCoordinator() {
            return coordinator;
        }
    }
}
