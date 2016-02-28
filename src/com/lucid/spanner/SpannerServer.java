package com.lucid.spanner;

import com.lucid.test.MapStateMachine;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

public class SpannerServer {

    private int clientPort;
    private int serverPort;
    private int paxosPort;

    List<Address> paxosMembers;

    private TwoPC twoPC;
    public ch.qos.logback.classic.Logger logger;

    Map<Integer, Integer> txnPrepares;


    SpannerServer(int clientPort, int serverPort, int paxosPort) {
        this.clientPort = clientPort;
        this.serverPort = serverPort;
        this.paxosPort = paxosPort;
        logger = SpannerUtils.root;
        twoPC = new TwoPC(this, logger);
        paxosMembers = new ArrayList<>();

        // Start Paxos cluster
        startPaxosCluster();

        // Start server accept thread
        acceptServers();

        // Start client accept thread
        acceptClients();

    }

    private void startPaxosCluster() {
        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("UnknownHostException while starting paxos cluser.");
            e.printStackTrace();
        }
        Address selfPaxosAddress = new Address(host, paxosPort);

        int index = -1;
        for (Address addr:Config.SERVER_IPS) {
            index++;
            if(SpannerUtils.isThisMyIpAddress(addr.host(), addr.port())){
                break;
            }
        }
        if(index >= Config.SERVER_IPS.size()){
            logger.error("could not find own ip in IpList!!");
        }

        int clusterSize = Config.SERVER_IPS.size()/Config.NUM_CLUSTERS; // Note: Assuming equal-sized clusters
        int position = index % clusterSize;

        for (int i=position; i<Config.SERVER_IPS.size(); i += clusterSize) {
            if(i!=index){
                paxosMembers.add(Config.SERVER_IPS.get(i));
            }
        }
        CopycatServer server = CopycatServer.builder(selfPaxosAddress, paxosMembers)
                .withTransport(new NettyTransport())
                .withStateMachine(MapStateMachine::new)
                .withStorage(new Storage("logs/" + host + paxosPort))
                .build();
        server.serializer().disableWhitelist();

        server.open().join();
    }

    private void acceptServers(){
        Runnable serverRunnable = new Runnable(){
            @Override
            public void run(){
                try {
                    while(true) {
                        ServerSocket serverSocket = new ServerSocket(serverPort);
                        Socket cohort = serverSocket.accept();
                        handleServerAccept(cohort);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        SpannerUtils.startThreadWithName(serverRunnable, "ServerAccept Thread at Server ");
    }

    /*   For 2PC, servers communicate among themselves as follows:
         1. Leaders who are not coordinators: Send PREPARE_ACK / PREPARE_NACK message once they are Prepared
             "PREPARE_ACK:tid" or
             "PREPARE_NACK:tid"
         2. Coordinator sends commit/abort after getting messages from everyone
             "COMMIT:tid" or
             "ABORT:tid"
     */
    private void handleServerAccept(Socket cohort){
        String inputLine = "";
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(cohort.getInputStream()));
            inputLine = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SpannerUtils.SERVER_MSG msgType;
        long tid;

        if(inputLine.startsWith("PREPARE_ACK")){
            msgType = SpannerUtils.SERVER_MSG.PREPARE_ACK;
        }
        else if(inputLine.startsWith("PREPARE_NACK")){
            msgType = SpannerUtils.SERVER_MSG.PREPARE_NACK;
        }
        else if(inputLine.startsWith("COMMIT")){
            msgType = SpannerUtils.SERVER_MSG.COMMIT;
        }
        else if(inputLine.startsWith("ABORT")){
            msgType = SpannerUtils.SERVER_MSG.ABORT;
        }
        else{
            logger.error("Unknown msg recd from cohort:"+inputLine);
            try {
                cohort.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        String[] msg = inputLine.split(":");
        tid = Integer.parseInt(msg[1]);

        if(msgType == SpannerUtils.SERVER_MSG.PREPARE_ACK){
            if(!iAmCoordinator()){
                logger.error("PrepareAck recd at non-coordinator");
            }
            else
                twoPC.recvPrepareAck(tid);
        }
        else if(msgType == SpannerUtils.SERVER_MSG.COMMIT) {
            if(iAmCohort() || iAmCoordinator()){
                logger.error("Commit recd at non-leader");
            }
            // Commit using Paxos in own cluster
            CommitCommand cmd = new CommitCommand(tid);
            paxosReplicate(cmd);

            // Release Locks
            releaseLocks(tid);
        }
        else if(msgType == SpannerUtils.SERVER_MSG.PREPARE_NACK){
            if(!iAmCoordinator()){
                logger.error("PrepareNack recd at non-coordinator");
            }
            else{
                twoPC.recvPrepareAck(tid);
            }
        }
        else if(msgType == SpannerUtils.SERVER_MSG.ABORT){
            if(iAmCohort() || iAmCoordinator()){
                logger.error("Abort recd at non-leader");
            }
            // Abort using Paxos in own cluster
            AbortCommand cmd = new AbortCommand(tid);
            paxosReplicate(cmd);

            // Release Locks
            releaseLocks(tid);
        }
    }


    private void acceptClients(){
        Runnable clientRunnable = new Runnable(){
            @Override
            public void run(){
                try {
                    while(true) {
                        ServerSocket serverSocket = new ServerSocket(clientPort);
                        Socket client = serverSocket.accept();
                        handleClientAccept(client);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        SpannerUtils.startThreadWithName(clientRunnable, "ClientAccept Thread at Server: ");
    }

    private void handleClientAccept(Socket client){
        Runnable clientRunnable = new Runnable(){
            @Override
            public void run() {
                try {
                    long tid = 0;
                    int nShards = 0;
                    // Send my role to the client
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println(getMyRole().ordinal());

                    // Check if client closes connection now. Then cleanup and exit.
                    if(client.isClosed()){
                        return;
                    }

                    // TODO: Read Command from client (as JSON), Make sure this is PREPARE msg
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String inputLine = in.readLine();

                    TransportObject transportObject = null;
                    // TODO: De-serialize Commit object, get tid

                    List <String> keys = new ArrayList<>();
                    List <Object> values = new ArrayList<>();
                    keys.add(transportObject.getKey());  // TODO: we should actually get list of keys and values
                    values.add(transportObject.getValue());
                    tid = transportObject.getTxn_id();

                    // Try to obtain locks (blocking)
                    obtainLocks(tid, keys);

                    // Replicate PrepareCommitCommand using Paxos.
                    PrepareCommitCommand prepCommand = new PrepareCommitCommand(keys, values);
                    paxosReplicate(prepCommand);

                    // If I am coordinator, wait till all are prepared.
                    if(iAmCoordinator()) {
                        // TODO: Determine number of shards involved
                        twoPC.addNewTxn(tid, nShards);
                        twoPC.waitForPrepareAcks(tid);
                        Address clientAddr = new Address(client.getInetAddress().getHostName(), client.getPort());
                        if(twoPC.canCommit(tid)) {
                            // send COMMIT to client
                            send2PCMsgSingle(SpannerUtils.SERVER_MSG.COMMIT, tid, clientAddr);
                            handleAllPrepareAcks(tid);
                        }
                        else{
                            // Send Abort to client
                            send2PCMsgSingle(SpannerUtils.SERVER_MSG.COMMIT, tid, clientAddr);
                            handlePrepareNack(tid);
                        }
                    }
                    else{
                        // TODO: get coordinator IP from request and send PREPARE_ACK / PREPARENACK
                        //Address coordAddr = new Address(transportObject.getCoordinator().host(), transportObject.getCoordinator().port());
                        Address coordAddr = new Address("", 1234);
                        send2PCMsgSingle(SpannerUtils.SERVER_MSG.PREPARE_ACK, tid, coordAddr);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        SpannerUtils.startThreadWithName(clientRunnable, "Client Handling thread for client:");
    }

    /* After receiving PREPARE_ACKs from everyone, txn is ready for commit.

     */
    private void handleAllPrepareAcks(long tid){
        // (Commit locally) Do Paxos in own cluster for CommitMsg
        CommitCommand cmd = new CommitCommand(tid);
        paxosReplicate(cmd);

        // Release Locks
        releaseLocks(tid);

        // Send 2PC commit to other leaders.
        send2PCMsg(SpannerUtils.SERVER_MSG.COMMIT, tid, null);

        // Remove transaction from list of active txns
        twoPC.removeTxn(tid);

    }

    private void handlePrepareNack(long tid){
        // (Abort locally) Do Paxos in own cluster for AbortMsg
        AbortCommand cmd = new AbortCommand(tid);
        paxosReplicate(cmd);

        // Release Locks
        releaseLocks(tid);

        // Send 2PC abort to other leaders.
        send2PCMsg(SpannerUtils.SERVER_MSG.ABORT, tid, null);

        // Remove transaction from list of active txns
        twoPC.removeTxn(tid);
    }

    private boolean iAmLeader(){
        SpannerUtils.ROLE role;
        role = getMyRole();
        return (role == SpannerUtils.ROLE.LEADER) || (role == SpannerUtils.ROLE.COORDINATOR);
    }

    private boolean iAmCoordinator(){
        SpannerUtils.ROLE role;
        role = getMyRole();
        return role == SpannerUtils.ROLE.COORDINATOR;
    }

    private boolean iAmCohort(){
        SpannerUtils.ROLE role;
        role = getMyRole();
        return role == SpannerUtils.ROLE.COHORT;
    }

    public SpannerUtils.ROLE getMyRole(){
        SpannerUtils.ROLE role = SpannerUtils.ROLE.COHORT;
        // TODO: Determine own role
        return role;
    }

    private void paxosReplicate(Command command){
        // Create CopyCat client and replicate command
        CopycatClient client = CopycatClient.builder(paxosMembers)
                .withTransport(new NettyTransport())
                .build();
        client.serializer().disableWhitelist();

        client.open().join();

        try {
            client.submit(command).get(Config.COMMAND_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        client.close().join();
    }

    private void send2PCMsg(SpannerUtils.SERVER_MSG msgType, long tid, List<Address> recipients) {
        // Send to all Leaders
        if (recipients == null) {
            for (Address addr : paxosMembers) {
                send2PCMsgSingle(msgType, tid, addr);
            }

        } else {
            // Send to given list
            for (Address addr : paxosMembers) {
                send2PCMsgSingle(msgType, tid, addr);
            }
        }
    }

    private void send2PCMsgSingle(SpannerUtils.SERVER_MSG msgType, long tid, Address addr) {
        String msg = msgType.toString()+":"+tid;
        try {
            Socket socket = new Socket(addr.host(), addr.port());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msg);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void obtainLocks(long tid, List<String> key){
        // TODO: get locks
    }

    private void releaseLocks(long tid){
        if(!iAmLeader()){
            logger.debug("I am not a leader :/");
            return;
        }
        // TODO: release locks
    }

    public static void main(String[] args) {

    }
}

class TState{
    public enum CSTATE{
        COMMIT, ABORT, UNKNOWN
    }
    int numShards;
    Semaphore prepareCount;
    CSTATE commit;
    //int commit_count;

    public TState(int nShards){
        numShards = nShards;
        prepareCount = new Semaphore(-1*nShards + 1);
        commit = CSTATE.UNKNOWN;
        //commit_count = 0;
    }

    @Override
    public String toString(){
        return "[TState] Shards:"+numShards+" PrepareCount"+prepareCount+"\n";
    }
}


class TwoPC{
    ConcurrentHashMap<Long, TState> txnState;
    public ch.qos.logback.classic.Logger logger;

    public TwoPC(SpannerServer spannerServer, ch.qos.logback.classic.Logger logger){
        txnState = new ConcurrentHashMap<>();
        this.logger = logger;
    }

    public void addNewTxn(long tid, int nShards){
        if(txnState.containsKey(tid)){
            logger.error("Add Txn called multiple times for same tid.");
            //TState tState = txnState.get(tid);
            //tState.numShards = nShards;  // In case where Coordinator receives prepare after other leaders
            return;
        }
        txnState.put(tid, new TState(nShards));
    }

    public TState removeTxn(long tid){
        return txnState.remove(tid);
    }

    public TState get(long tid){
        return txnState.get(tid);
    }

    public boolean isActive(long tid){
        return txnState.containsKey(tid);
    }

    public void waitForPrepareAcks(long tid){
        TState tState = txnState.get(tid);
        int nshards = tState.numShards;
        try {
            tState.prepareCount.acquire(nshards);
            if(tState.commit != TState.CSTATE.ABORT)    // Haven't recd NACK
                tState.commit = TState.CSTATE.COMMIT;
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.info("Interrupted while waiting for prepared. tid:"+tid);
        }
    }

    public void recvPrepareAck(long tid){
        if(!txnState.containsKey(tid)){
            // When prepare ack from other Leaders is recd after prepare from client
            logger.error("Recd prepareAck but Txn does not exist tid:"+tid);
            return;
            //addNewTxn(tid, Config.NUM_CLUSTERS);
        }
        TState tState = txnState.get(tid);
        tState.prepareCount.release();
    }

    public void recvPrepareNack(long tid){
        if(!txnState.containsKey(tid)){
            // When prepare ack from other Leaders is recd after prepare from client
            logger.error("Recd prepareNack but Txn does not exist tid:"+tid);
            return;
            //addNewTxn(tid, Config.NUM_CLUSTERS);
        }
        TState tState = txnState.get(tid);
        tState.commit = TState.CSTATE.ABORT;
        tState.prepareCount.release(tState.numShards);  // Semaphore released immediately to let know waiting server
    }

    public boolean canCommit(long tid){
        TState tState = txnState.get(tid);
        if(tState.commit == TState.CSTATE.COMMIT)
            return true;
        else
            return false;
    }
}
