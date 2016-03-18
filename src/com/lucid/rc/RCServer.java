package com.lucid.rc;

import com.lucid.common.*;
import com.lucid.rc.ServerMsg.Message;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    private final Map<AddressConfig, ObjectOutputStream> datacenterOutputStreams = new HashMap<>();

    private final Map<AddressConfig, ObjectOutputStream> replicaOutputStreams = new HashMap<>();

    private final Map<Long, List<Semaphore>> txnLocks = new ConcurrentHashMap<>();

    private final Map<Long, Semaphore> ackLocks = new ConcurrentHashMap<>();

    private final Map<Long, AtomicInteger> counter = new ConcurrentHashMap<>();

    private final StripedExclusiveSemaphore stripedSemaphore = new StripedExclusiveSemaphore(Config.LOCK_TABLE_SIZE);

    public RCServer(AddressConfig addressConfig, int index) {
        Lucid.getInstance().onServerStarted();
        LOG_TAG = "RC_SERVER-" + index;

        LogUtils.debug(LOG_TAG, "Initializing " + addressConfig);

        this.addressConfig = addressConfig;
        this.clientPort = addressConfig.getClientPort();
        this.serverPort = addressConfig.getServerPort();
        this.stateMachine = new RCStateMachine();
        this.datacenterIPs = Utils.getDatacenterIPs(index);
        this.replicasIPs = Utils.getReplicaIPs(index);

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
                    readServerMsg(ss.accept());
                }
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in accept-server thread", e);
            }
        };

        Utils.startThreadWithName(runnable, "accept-server");
    }

    private void readServerMsg(Socket server) {
        Runnable runnable = () -> {
            try {
                ObjectInputStream inputStream = new ObjectInputStream(server.getInputStream());
                while (true) {
                    ServerMsg msg = (ServerMsg) inputStream.readObject();
                    handleServerMsg(msg);
                }
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in read-server-msg thread", e);
            } finally {
                Utils.closeQuietly(server);
            }
        };

        Utils.startThreadWithName(runnable, "read-server-msg");
    }

    private void handleServerMsg(ServerMsg msg) {
        Runnable runnable = () -> {
            LogUtils.debug(LOG_TAG, "Handling server msg: " + msg);

            try {
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
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in handle-server-msg thread", e);
            }
        };

        Utils.startThreadWithName(runnable, "handle-server-msg");
    }

    private synchronized void acquireTxnLocks(ServerMsg msg) throws Exception {
        List<Semaphore> lockList;
        lockList = txnLocks.get(msg.getTxn_id());
        if (lockList == null) {
            lockList = new ArrayList<>();
            for (String key : msg.getMap().keySet()) {
                if (serverContainsKey(addressConfig, key)) {
                    Semaphore semaphore = stripedSemaphore.get(key);
                    lockList.add(semaphore);
                }
            }
            txnLocks.put(msg.getTxn_id(), lockList);
        }

        for (Semaphore semaphore : lockList) {
            semaphore.acquire();
        }
    }

    private void releaseTxnLocks(ServerMsg msg) throws Exception {
        List<Semaphore> lockList;
        lockList = txnLocks.get(msg.getTxn_id());
        if (lockList == null) {
            lockList = new ArrayList<>();
            for (String key : msg.getMap().keySet()) {
                if (serverContainsKey(addressConfig, key)) {
                    Semaphore semaphore = stripedSemaphore.get(key);
                    lockList.add(semaphore);
                }
            }
        }

        for (Semaphore semaphore : lockList) {
            semaphore.release();
        }
    }

    // Received by all the servers
    private void handle2PCPrepare(ServerMsg msg) throws Exception {
        // Take locks
        acquireTxnLocks(msg);

        // Send 2PC ack to coordinator
        AddressConfig coordinator = msg.getCoordinator();
        ObjectOutputStream oos = datacenterOutputStreams.get(coordinator);
        ServerMsg ackMsg = new ServerMsg(msg, Message.ACK_2PC_PREPARE);

        Thread.sleep(Config.INTRA_DATACENTER_LATENCY);
        synchronized (oos) {
            oos.writeObject(ackMsg);
        }
    }

    // Only received by a coordinator
    private void handleAck2PCPrepare(ServerMsg msg) throws Exception {
        if (!msg.getCoordinator().equals(addressConfig)) {
            throw new Exception("Unexpected msg received: " + msg);
        }

        // Let the coordinator know that ack is received
        ackLocks.get(msg.getTxn_id()).release();
    }

    // Only received by a coordinator
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
            ServerMsg commitMsg = new ServerMsg(msg, Message._2PC_COMMIT);

            // Send 2PC Commit to datacenter servers
            Set<AddressConfig> datacenterIPs = getDatacenterIPsFor(msg.getMap());
            for (AddressConfig config : datacenterIPs) {
                ObjectOutputStream oos = datacenterOutputStreams.get(config);

                Thread.sleep(Config.INTRA_DATACENTER_LATENCY);
                synchronized (oos) {
                    oos.writeObject(commitMsg);
                }
            }
        }

        // Remove counter
        if (count == total) {
            synchronized (counter) {
                counter.remove(msg.getTxn_id());
            }
        }
    }

    // Received by all the servers
    private void handle2PCCommit(ServerMsg msg) throws Exception {
        // Commit values
        stateMachine.write(msg.getTxn_id(), createMyWriteMap(msg.getMap()));

        // Release txn locks
        releaseTxnLocks(msg);
    }

    private void openClientPort() {
        Runnable runnable = () -> {
            try {
                ServerSocket ss = new ServerSocket(clientPort);
                while (true) {
                    readClientMsg(ss.accept());
                }
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in accept-client thread", e);
            }
        };

        Utils.startThreadWithName(runnable, "accept-client");
    }

    private void readClientMsg(Socket client) {
        Runnable runnable = () -> {
            TransportObject msg = null;

            try {
                ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
                msg = (TransportObject) inputStream.readObject();

                // Create semaphore
                ackLocks.put(msg.getTxn_id(), new Semaphore(0));

                handleClientMsg(msg, outputStream);
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in read-client thread", e);
            } finally {
                Utils.closeQuietly(client);

                // Clear semaphore
//                if (msg != null) {
//                    ackLocks.remove(msg.getTxn_id());
//                }
            }
        };

        Utils.startThreadWithName(runnable, "read-client");
    }

    private void handleClientMsg(TransportObject msg, ObjectOutputStream outputStream) throws Exception {
        LogUtils.debug(LOG_TAG, "Handling client msg: " + msg);

        if (msg.getKey() != null) {
            // Read query
            Pair<Long, String> value = stateMachine.read(msg.getKey());

            Thread.sleep(Config.RC_CLIENT_TO_DATACENTER_AVG_LATENCY);
            outputStream.writeObject(value);
        } else {
            // Write command
            ServerMsg serverMsg = new ServerMsg(null, msg.getKey(), msg.getWriteMap(), msg.getTxn_id(), addressConfig);

            // Send 2PC PREPARE to shards within datacenter
            ServerMsg _2PCPrepare = new ServerMsg(serverMsg, Message._2PC_PREPARE);
            Set<AddressConfig> datacenterIPs = getDatacenterIPsFor(msg.getWriteMap());
            Thread.sleep(Config.INTRA_DATACENTER_LATENCY);
            for (AddressConfig config : datacenterIPs) {
                ObjectOutputStream oos = datacenterOutputStreams.get(config);
                synchronized (oos) {
                    oos.writeObject(_2PCPrepare);
                }
            }

            // Wait for ack 2PC from datacenter servers
            ackLocks.get(msg.getTxn_id()).acquire(datacenterIPs.size());

            // Inform client
            Pair<Long, String> value = new Pair<>(msg.getTxn_id(), "COMMIT");
            Thread.sleep(Config.RC_CLIENT_TO_DATACENTER_AVG_LATENCY);
            outputStream.writeObject(value);

            // Inform other coordinators
            ServerMsg ack2PCPrepare = new ServerMsg(serverMsg, Message._2PC_ACCEPT);
            for (ObjectOutputStream oos : replicaOutputStreams.values()) {
                Thread.sleep(Config.RC_INTER_DATACENTER_LATENCY);
                synchronized (oos) {
                    oos.writeObject(ack2PCPrepare);
                }
            }
        }
    }

    private Set<AddressConfig> getDatacenterIPsFor(Map<String, String> writeMap) {
        Set<AddressConfig> ips = new HashSet<>();
        for (String key : writeMap.keySet()) {
            for (AddressConfig config : datacenterIPs) {
                if (serverContainsKey(config, key)) {
                    ips.add(config);
                }
            }
        }
        return ips;
    }

    private boolean serverContainsKey(AddressConfig config, String key) {
        List<AddressConfig> ips = Utils.getReplicaClusterIPs(key);
        return ips.contains(config);
    }

    private Map<String, String> createMyWriteMap(Map<String, String> writeMap) {
        Map<String, String> map = new HashMap<>();
        for (String key : writeMap.keySet()) {
            if (serverContainsKey(addressConfig, key)) {
                map.put(key, writeMap.get(key));
            }
        }
        return map;
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
}
