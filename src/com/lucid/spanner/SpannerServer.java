package com.lucid.spanner;

import com.google.common.util.concurrent.Striped;
import com.lucid.common.LogUtils;
import com.lucid.common.Lucid;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

public class SpannerServer {

    private final String LOG_TAG;
    private int index;
    private String host;
    private int clientPort;
    private int serverPort;
    private int paxosPort;

    private Map<Long, Lock[]> lockMap;

    private CopycatClient copycatClient;

    volatile private CopycatServer.State role;

    List<AddressConfig> paxosMembers;

    private TwoPC twoPC;

    private final Striped<Lock> stripedLocks = Striped.lazyWeakLock(100);

    public SpannerServer(AddressConfig addressConfig, int index) {
        Lucid.getInstance().onServerStarted();
        LOG_TAG = "SPANNER_SERVER-" + index;

        this.index = index;
        this.host = addressConfig.host();
        this.serverPort = addressConfig.getServerPort();
        this.clientPort = addressConfig.getClientPort();
        this.paxosPort = addressConfig.port();
        this.lockMap = new HashMap<>();

        twoPC = new TwoPC();
        paxosMembers = new ArrayList<>();
        LogUtils.debug(LOG_TAG, "Initialing SpannerServer with host:" + host +
                " paxosPort:" + paxosPort + " serverPort:" + serverPort + " clientPort:" + clientPort);

        // Start Paxos cluster
        startPaxosCluster();

        this.copycatClient = SpannerUtils.buildClient(SpannerUtils.toAddress(paxosMembers));
        this.copycatClient.open().join();

        // Start server accept thread
        acceptServers();

        // Start client accept thread
        acceptClients();

    }

    private void startPaxosCluster() {
        Address selfPaxosAddress = new Address(host, paxosPort);

        paxosMembers = SpannerUtils.getPaxosClusterAll(index);
        LogUtils.debug(LOG_TAG, "Starting SpannerServer at:" + host + " with members:" + paxosMembers);

        CopycatServer server = CopycatServer.builder(selfPaxosAddress, SpannerUtils.toAddress(paxosMembers))
                .withTransport(new NettyTransport())
                .withStateMachine(SpannerStateMachine::new)
                .withStorage(Storage.builder().withStorageLevel(StorageLevel.MEMORY).
                        withDirectory(String.valueOf(paxosPort)).build())
                .build();

        server.onStateChange(new Consumer<CopycatServer.State>() {
            @Override
            public void accept(CopycatServer.State state) {
                role = state;
                LogUtils.debug(LOG_TAG, "State updated: " + role.name());
            }
        });
        server.serializer().disableWhitelist();

        server.open().join();
        LogUtils.debug(LOG_TAG, "Started SpannerServer at:" + host + ":" + paxosPort);
    }

    private void acceptServers() {
        Runnable serverRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(serverPort);
                    LogUtils.debug(LOG_TAG, "Starting serverSocket at server port");
                    while (true) {
                        Socket cohort = serverSocket.accept();
                        LogUtils.debug(LOG_TAG, "Received msg at ServerPort from:" +
                                cohort.getInetAddress() + cohort.getPort());
                        handleServerAccept(cohort);
                    }
                } catch (Exception e) {
                    LogUtils.error(LOG_TAG, "Exception in acceptServer thread.", e);
                }
            }
        };
        SpannerUtils.startThreadWithName(serverRunnable, "ServerAccept Thread at Server " + host + ":" + serverPort);
        LogUtils.debug(LOG_TAG, "Started thread for accepting cohort messages at " + host + ":" + serverPort);
    }

    /*   For 2PC, servers communicate among themselves as follows:
         1. Leaders who are not coordinators: Send PREPARE_ACK / PREPARE_NACK message once they are Prepared
             "PREPARE_ACK:tid" or
             "PREPARE_NACK:tid"
         2. Coordinator sends commit/abort after getting messages from everyone
             "COMMIT:tid" or
             "ABORT:tid"
     */
    private void handleServerAccept(Socket cohort) {
        String inputLine = "";
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(cohort.getInputStream()));
            inputLine = in.readLine();
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Error while communicating with cohort", e);
        }

        SpannerUtils.SERVER_MSG msgType;
        long tid;

        if (inputLine.startsWith("PREPARE_ACK")) {
            msgType = SpannerUtils.SERVER_MSG.PREPARE_ACK;
        } else if (inputLine.startsWith("PREPARE_NACK")) {
            msgType = SpannerUtils.SERVER_MSG.PREPARE_NACK;
        } else if (inputLine.startsWith("COMMIT")) {
            msgType = SpannerUtils.SERVER_MSG.COMMIT;
        } else if (inputLine.startsWith("ABORT")) {
            msgType = SpannerUtils.SERVER_MSG.ABORT;
        } else {
            LogUtils.error(LOG_TAG, "Unknown msg recd from cohort:" + inputLine);
            try {
                cohort.close();
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Exception while closing connection.", e);
            }
            return;
        }

        String[] msg = inputLine.split(":");
        tid = Integer.parseInt(msg[1]);
        LogUtils.debug(LOG_TAG, "Received 2PC msg of type:" + msgType + " for TxnID:" + tid);

        switch (msgType) {
            case PREPARE_ACK:
                if (!iAmCoordinator(tid)) {
                    LogUtils.error(LOG_TAG, "PrepareAck recd at non-coordinator from server " + cohort.getInetAddress
                            ().getHostName() + cohort.getPort());
                }
                twoPC.recvPrepareAck(tid);

                break;

            case COMMIT:
                if (!iAmLeader()) {
                    LogUtils.error(LOG_TAG, "Commit recd at non-leader");
                }
                // Commit using Paxos in own cluster
                // Could it be older msg?
                CommitCommand cmd = new CommitCommand(tid);
                paxosReplicate(cmd);

                // Release Locks
                releaseLocks(tid);
                break;

            case PREPARE_NACK:
                if (!iAmCoordinator(tid)) {
                    LogUtils.error(LOG_TAG, "PrepareNack recd at non-coordinator");
                }
                twoPC.recvPrepareAck(tid);
                break;

            case ABORT:
                if (!iAmLeader()) {
                    LogUtils.error(LOG_TAG, "Abort recd at non-leader");
                }
                // Abort using Paxos in own cluster
                AbortCommand acmd = new AbortCommand(tid);
                paxosReplicate(acmd);

                // Release Locks
                releaseLocks(tid);
                break;

            default:
                break;
        }

    }

    private void acceptClients() {
        Runnable clientRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(clientPort);
                    while (true) {
                        Socket client = serverSocket.accept();
                        handleClientAccept(client);
                    }
                } catch (Exception e) {
                    LogUtils.error(LOG_TAG, "Exception in accept Client thread.", e);
                }
            }
        };
        SpannerUtils.startThreadWithName(clientRunnable, "ClientAccept Thread at Server " + host + ":" + clientPort);
        LogUtils.debug(LOG_TAG, "Started thread for accepting client messages at " + host + ":" + clientPort);
    }

    private void handleClientAccept(Socket client) {
        Runnable clientRunnable = new Runnable() {
            @Override
            public void run() {
                long tid = 0;
                int nShards = 0;
                try {
                    LogUtils.debug(LOG_TAG, "Client connected:" + client.getInetAddress() + ":" + client.getPort());
                    // Send my role to the client
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println(iAmLeader() ? "1" : "0");

                    // If I am not leader, close connection, cleanup and exit.
                    if (!iAmLeader()) {
                        LogUtils.debug(LOG_TAG, "Sent client msg that I am not leader. Closing connection.");
                        com.lucid.common.Utils.closeQuietly(client);
                        return;
                    }

                    // Get de-serialized TransportObject from client
                    //BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    //String inputLine = in.readLine();

                    ObjectInputStream objStream = new ObjectInputStream(client.getInputStream());
                    TransportObject transportObject = null;
                    try {
                        transportObject = (TransportObject) objStream.readObject();
                        if (transportObject == null) {
                            LogUtils.error(LOG_TAG, "Received Null TransportObject from:" + client.getInetAddress());
                            throw new NullPointerException();
                        }
                    } catch (ClassNotFoundException e) {
                        LogUtils.error(LOG_TAG, "Recd something other than TransportObject", e);
                    } catch (NullPointerException e) {
                        LogUtils.error(LOG_TAG, "De-serialized Transport object is null.", e);
                    }

                    tid = transportObject.getTxn_id();
                    Map<String, String> writeMap = transportObject.getWriteMap();
                    LogUtils.debug(LOG_TAG, "Received writeMap for Txn ID:" + tid + " map:" + writeMap);

                    // Try to obtain locks (blocking)
                    obtainLocks(tid, writeMap.keySet());

                    // Replicate PrepareCommitCommand using Paxos.
                    PrepareCommitCommand prepCommand = new PrepareCommitCommand(writeMap);
                    LogUtils.debug(LOG_TAG, "Replicating PrepareCommitCommand for tid: " + tid);
                    paxosReplicate(prepCommand);

                    // If I am coordinator, wait till all are prepared.
                    if (transportObject.isCoordinator()) {
                        LogUtils.debug(LOG_TAG, "At Coordinator. Finished Replicating. Waiting for Prepare(N)ACKs.");
                        // Determine number of shards involved
                        nShards = transportObject.getNumber_of_leaders() - 1;
                        twoPC.addNewTxn(tid, nShards);
                        twoPC.waitForPrepareAcks(tid);
                        Address clientAddr = new Address(client.getInetAddress().getHostName(), client.getPort());
                        if (twoPC.canCommit(tid)) {
                            handleAllPrepareAcks(tid, client);
                        } else {
                            handlePrepareNack(tid, client);
                        }
                    } else {
                        LogUtils.debug(LOG_TAG, "At Leader: Sending prepare ACK/NACK to co-ordinator.");
                        // Get coordinator IP from request and send PREPARE_ACK / PREPARENACK
                        AddressConfig coordPaxosAddr = (AddressConfig) transportObject.getCoordinator();
                        // Get corresponding server port
                        Address coordServerAddr = new Address(coordPaxosAddr.host(), coordPaxosAddr.getServerPort());
                        send2PCMsgSingle(SpannerUtils.SERVER_MSG.PREPARE_ACK, tid, coordServerAddr);
                    }

                } catch (Exception e) {
                    LogUtils.error(LOG_TAG, "Error during 2PC handling.", e);
                } finally {
                    tryReleaseLocks(tid);
                    if (client != null) {
                        try {
                            client.close();
                        } catch (Exception e) {
                            LogUtils.error(LOG_TAG, "Exception while closing client.", e);
                        }
                    }
                }
            }
        };
        SpannerUtils.startThreadWithName(clientRunnable, "Client Handling thread for client:" + client.getInetAddress());
    }

    /* After receiving PREPARE_ACKs from everyone, txn is ready for commit.

     */
    private void handleAllPrepareAcks(long tid, Socket client) {
        LogUtils.debug(LOG_TAG, "Coord recd all ACKs");
        // (Commit locally) Do Paxos in own cluster for CommitMsg
        CommitCommand cmd = new CommitCommand(tid);
        paxosReplicate(cmd);
        LogUtils.debug(LOG_TAG, "Replicated Commit Command at Coord.");

        // Release Locks
        releaseLocks(tid);

        LogUtils.debug(LOG_TAG, "Sending 2PC COMMIT message to client and other leaders.");
        // send COMMIT to client
        send2PCMsgSingle(SpannerUtils.SERVER_MSG.COMMIT, tid, client);

        // Send 2PC commit to other leaders.
        send2PCMsg(SpannerUtils.SERVER_MSG.COMMIT, tid, null);

        // Remove transaction from list of active txns
        twoPC.removeTxn(tid);
    }


    private void handlePrepareNack(long tid, Socket client) {
        LogUtils.debug(LOG_TAG, "Coord recd a NACK. Sending ABORT.");
        // (Abort locally) Do Paxos in own cluster for AbortMsg
        AbortCommand cmd = new AbortCommand(tid);
        paxosReplicate(cmd);

        LogUtils.debug(LOG_TAG, "Replicated AbortCommand at Coord.");

        // Release Locks
        releaseLocks(tid);

        LogUtils.debug(LOG_TAG, "Sending 2PC ABORTs to client and other leaders.");
        // send COMMIT to client
        send2PCMsgSingle(SpannerUtils.SERVER_MSG.ABORT, tid, client);

        // Send 2PC abort to other leaders.
        send2PCMsg(SpannerUtils.SERVER_MSG.ABORT, tid, null);

        // Remove transaction from list of active txns
        twoPC.removeTxn(tid);
    }

    private boolean iAmLeader() {
        return this.role == CopycatServer.State.LEADER;
    }

    private boolean iAmCoordinator(long tid) {
        return twoPC.isActive(tid);
    }

    private void paxosReplicate(Command command) {
        // Create CopyCat client and replicate command

        try {
            LogUtils.debug(LOG_TAG, "Paxos Replication. Creating Client.");
            copycatClient.submit(command).get(Config.COMMAND_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Exception while replicating Command:" + command, e);
        }
    }

    private void send2PCMsg(SpannerUtils.SERVER_MSG msgType, long tid, List<AddressConfig> recipients) {
        // Send to all Leaders
        if (recipients == null) {
            for (Address addr : SpannerUtils.toAddress(paxosMembers)) {
                send2PCMsgSingle(msgType, tid, addr);
            }

        } else {
            // Send to given list
            for (Address addr : SpannerUtils.toAddress(paxosMembers)) {
                send2PCMsgSingle(msgType, tid, addr);
            }
        }
    }

    private void send2PCMsgSingle(SpannerUtils.SERVER_MSG msgType, long tid, Address client) {
        String msg = msgType.toString() + ":" + tid;
        try {
            Socket socket = new Socket(client.host(), client.port());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
            LogUtils.debug(LOG_TAG, "Sent 2PC Message to " + client.host());
            socket.close();
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Exception during sending 2PC msg:", e);
        }
    }

    private void send2PCMsgSingle(SpannerUtils.SERVER_MSG msgType, long tid, Socket socket) {
        if (socket == null) {
            LogUtils.error(LOG_TAG, "Socket object is NULL.");
            throw new NullPointerException();
        }
        String msg = msgType.toString() + ":" + tid;
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
            //socket.close();
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Exception during sending 2PC msg:", e);
        }
    }

    private void obtainLocks(long tid, Set<String> keys) {
        int numberOfLocks = keys.size();
        int counter = 0;
        Lock[] locks = new Lock[numberOfLocks];
        LogUtils.debug(LOG_TAG, "Getting locks for txn:" + tid + " keys:" + keys);
        try {
            LogUtils.debug(LOG_TAG, "Txn " + tid + " is waiting for locks.");
            for (String key : keys) {
                locks[counter] = stripedLocks.get(key);
                locks[counter].lock();
                counter++;
            }
            LogUtils.debug(LOG_TAG, "Txn " + tid + " got all locks.");
            lockMap.put(tid, locks);
        }
        // TODO: need to handle locks in a better way. for ex. if txn fails, locks should be released
        catch (Exception e) {
            LogUtils.error(LOG_TAG, "Exception while locking for txn tid:" + tid, e);
            releaseLocks(tid);
        }
    }

    private void tryReleaseLocks(long tid) {
        if (lockMap.containsKey(tid))
            releaseLocks(tid);
    }

    private void releaseLocks(long tid) {
        if (!iAmLeader()) {
            LogUtils.error(LOG_TAG, "I am not a leader, but I am in releaseLocks :/");
            return;
        }
        LogUtils.debug(LOG_TAG, "Txn " + tid + " is releasing all locks.");
        // TODO: release locks
        if (lockMap.containsKey(tid)) {
            Lock[] locks = lockMap.get(tid);
            for (Lock lock : locks) {
                try {
                    lock.unlock();
                } catch (Exception e) {
                    LogUtils.error(LOG_TAG, "Exception while Unlocking for txn tid:" + tid, e);
                    //TODO: add this lock to a global Unlocks list that should periodically run to release unheld locks
                }
                lockMap.remove(tid);
            }
        } else {
            LogUtils.error(LOG_TAG, "No Entry found in lockMap for txn: " + tid);
            return;
        }
        LogUtils.debug(LOG_TAG, "Txn " + tid + " released all locks.");
        return;
    }

    public static void main(String[] args) {
        int index = Integer.parseInt(args[0]);
        AddressConfig config = Config.SERVER_IPS.get(index);
        SpannerServer server = new SpannerServer(config, index);
    }
}

class TState {
    public enum CSTATE {
        COMMIT, ABORT, UNKNOWN
    }

    int numShards;
    Semaphore prepareCount;
    CSTATE commit;
    //int commit_count;

    public TState(int nShards) {
        numShards = nShards;
        prepareCount = new Semaphore(-1 * nShards + 1);
        commit = CSTATE.UNKNOWN;
        //commit_count = 0;
    }

    @Override
    public String toString() {
        return "[TState] Shards:" + numShards + " PrepareCount" + prepareCount + "\n";
    }
}


class TwoPC {
    private ConcurrentHashMap<Long, TState> txnState;
    private final String LOG_TAG = "TWO_PC";

    public TwoPC() {
        txnState = new ConcurrentHashMap<>();
    }

    public void addNewTxn(long tid, int nShards) {
        LogUtils.debug(LOG_TAG, "Adding txn " + tid + " to active map");
        if (txnState.containsKey(tid)) {
            LogUtils.error(LOG_TAG, "Add Txn called multiple times for same tid.");
            //TState tState = txnState.get(tid);
            //tState.numShards = nShards;  // In case where Coordinator receives prepare after other leaders
            return;
        }
        txnState.put(tid, new TState(nShards));
    }

    public TState removeTxn(long tid) {
        LogUtils.debug(LOG_TAG, "Removing txn " + tid + " from active map.");
        return txnState.remove(tid);
    }

    public TState get(long tid) {
        return txnState.get(tid);
    }

    public boolean isActive(long tid) {
        return txnState.containsKey(tid);
    }

    public void waitForPrepareAcks(long tid) {
        TState tState = txnState.get(tid);
        int nshards = tState.numShards;
        try {
            tState.prepareCount.acquire(nshards);
            if (tState.commit != TState.CSTATE.ABORT)    // Haven't recd NACK
                tState.commit = TState.CSTATE.COMMIT;
        } catch (Exception e) {
            LogUtils.error(LOG_TAG, "Interrupted while waiting for prepared. tid:" + tid, e);
        }
    }

    public void recvPrepareAck(long tid) {
        if (!txnState.containsKey(tid)) {
            // When prepare ack from other Leaders is recd after prepare from client
            LogUtils.error(LOG_TAG, "Recd prepareAck but Txn does not exist tid:" + tid);
            //return;
            addNewTxn(tid, Config.NUM_CLUSTERS);
        }
        TState tState = txnState.get(tid);
        tState.prepareCount.release();
    }

    public void recvPrepareNack(long tid) {
        if (!txnState.containsKey(tid)) {
            // When prepare ack from other Leaders is recd after prepare from client
            LogUtils.error(LOG_TAG, "Recd prepareNack but Txn does not exist tid:" + tid);
            return;
            //addNewTxn(tid, Config.NUM_CLUSTERS);
        }
        TState tState = txnState.get(tid);
        tState.commit = TState.CSTATE.ABORT;
        tState.prepareCount.release(tState.numShards);  // Semaphore released immediately to let know waiting server
    }

    public boolean canCommit(long tid) {
        TState tState = txnState.get(tid);
        if (tState.commit == TState.CSTATE.COMMIT)
            return true;
        else
            return false;
    }
}