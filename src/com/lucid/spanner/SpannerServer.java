package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class SpannerServer {

    private int clientPort;
    private int serverPort;
    private int paxosPort;

    private TwoPC twoPC;
    public ch.qos.logback.classic.Logger logger;

    Map<Integer, Integer> txnPrepares;


    SpannerServer(int clientPort, int serverPort, int paxosPort) throws UnknownHostException {
        this.clientPort = clientPort;
        this.serverPort = serverPort;
        this.paxosPort = paxosPort;
        logger = SpannerUtils.root;
        twoPC = new TwoPC(this, logger);

        // TODO: Start Paxos cluster
        String host = InetAddress.getLocalHost().getHostName();
        Address selfPaxosAddress = new Address(host, paxosPort);

        for (Address addr:Config.SERVER_IPS) {
            //if(SpannerUtils.isThisMyIpAddress((InetAddress) addr)){

            //}

        }

        // Start server accept thread
        acceptServers();

        // Start client accept thread
        acceptClients();

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

    private void handleServerAccept(Socket cohort){
        // TODO: Parse and determine msg type, tid
        SpannerUtils.SERVER_MSG msg = SpannerUtils.SERVER_MSG.PREPAREACK;
        int tid = 0;
        int nShards = 0;

        if(msg == SpannerUtils.SERVER_MSG.PREPAREACK){
            if(!iAmCoordinator()){
                logger.error("Prepare recd at non-coordinator");
            }
            else
                twoPC.recvPrepareAck(tid);
        }
        else if(msg == SpannerUtils.SERVER_MSG.COMMIT) {
            if(iAmCohort() || iAmCoordinator()){
                logger.error("Commit recd at non-leader");
            }
            // TODO: commit using Paxos
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
                    int tid = 0;
                    int nShards = 0;
                    // Send my role to the client
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    out.println(getMyRole());

                    // TODO: should ideally check if client closes connection now. Then cleanup and exit.

                    // TODO: Read Command from client (as JSON), Make sure this is PREPARE msg
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String inputLine = in.readLine();
                    // TODO: De-serialize Commit object, get tid

                    // TODO: Try to obtain locks (blocking)

                    // TODO: Create CopycatClient; create and submit PrepareCommitCommand. Blocking wait.

                    // If I am coordinator, wait till all are prepared.
                    if(iAmCoordinator()) {
                        // TODO: Determine number of shards involved
                        twoPC.addNewTxn(tid, nShards);
                        twoPC.waitForPrepareAcks(tid);
                        handlePrepareAck(tid);
                    }
                    else{
                        // TODO: get coordinator IP from request and send PrepareAck
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        SpannerUtils.startThreadWithName(clientRunnable, "Client Handling thread for client:");
    }

    private void handlePrepareAck(int tid){
        // TODO: Commit locally
        // TODO: Do Paxos in own cluster for CommitMsg
        // TODO: Release Locks
        // TODO: Send 2PC commit to client and other leaders.
        twoPC.removeTxn(tid);

    }

    private boolean iAmLeader(){
        SpannerUtils.ROLE role = SpannerUtils.ROLE.COHORT;
        role = getMyRole();
        return (role == SpannerUtils.ROLE.LEADER) || (role == SpannerUtils.ROLE.COORDINATOR);
    }

    private boolean iAmCoordinator(){
        SpannerUtils.ROLE role = SpannerUtils.ROLE.COHORT;
        role = getMyRole();
        return role == SpannerUtils.ROLE.COORDINATOR;
    }

    private boolean iAmCohort(){
        SpannerUtils.ROLE role = SpannerUtils.ROLE.COHORT;
        role = getMyRole();
        return role == SpannerUtils.ROLE.COHORT;
    }

    public SpannerUtils.ROLE getMyRole(){
        SpannerUtils.ROLE role = SpannerUtils.ROLE.COHORT;
        // TODO: Determine own role
        return role;
    }



    public static void main(String[] args) {

    }
}

class TState{
    int numShards;
    Semaphore prepareCount;
    //int prepareCount;
    //int commit_count;

    public TState(int nShards){
        numShards = nShards;
        prepareCount = new Semaphore(-1*nShards + 1);
        //commit_count = 0;
    }

    @Override
    public String toString(){
        return "[TState] Shards:"+numShards+" PrepareCount"+prepareCount+"\n";
    }
}


class TwoPC{
    ConcurrentHashMap<Integer, TState> txnState;
    public ch.qos.logback.classic.Logger logger;

    public TwoPC(SpannerServer spannerServer, ch.qos.logback.classic.Logger logger){
        txnState = new ConcurrentHashMap<>();
        this.logger = logger;
    }

    public void addNewTxn(int tid, int nShards){
        if(txnState.containsKey(tid)){
            logger.error("Add Txn called multiple times for same tid.");
            //TState tState = txnState.get(tid);
            //tState.numShards = nShards;  // In case where Coordinator receives prepare after other leaders
            return;
        }
        txnState.put(tid, new TState(nShards));
    }

    public TState removeTxn(int tid){
        return txnState.remove(tid);
    }

    public TState get(int tid){
        return txnState.get(tid);
    }

    public boolean isActive(int tid){
        return txnState.containsKey(tid);
    }

    public void waitForPrepareAcks(int tid){
        TState tState = txnState.get(tid);
        int nshards = tState.numShards;
        try {
            tState.prepareCount.acquire(nshards);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.info("Interrupted while waiting for prepared. tid:"+tid);
        }
    }

    public void recvPrepareAck(int tid){
        if(!txnState.containsKey(tid)){
            // When prepare ack from other Leaders is recd after prepare from client
            logger.error("Recd prepare but Txn does not exist tid:"+tid);
            return;
            //addNewTxn(tid, Config.NUM_CLUSTERS);
        }
        TState tstate = txnState.get(tid);
        tstate.prepareCount.release();
    }
}
