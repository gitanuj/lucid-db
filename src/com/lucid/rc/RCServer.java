package com.lucid.rc;

import com.lucid.common.*;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RCServer {

    private final String LOG_TAG;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private final AddressConfig addressConfig;

    private final int clientPort;

    private final int serverPort;

    private List<AddressConfig> datacenterIPs;

    private List<AddressConfig> replicasIPs;

    private List<Socket> datacenterSockets;

    private List<Socket> replicaSockets;

    public RCServer(AddressConfig addressConfig, int index) {
        Lucid.getInstance().onServerStarted();
        LOG_TAG = "RC_SERVER-" + index;

        LogUtils.debug(LOG_TAG, "Initializing " + addressConfig);

        this.addressConfig = addressConfig;
        this.clientPort = addressConfig.getClientPort();
        this.serverPort = addressConfig.getServerPort();
        this.datacenterSockets = new ArrayList<>();
        this.replicaSockets = new ArrayList<>();
        this.datacenterIPs = Utils.getDatacenterIPs(index);
        this.replicasIPs = Utils.getReplicaClusterIPs(index % Config.NUM_CLUSTERS);

        openServerPort();
        openClientPort();

        connectToDatacenter();
        connectToReplicas();
    }

    synchronized private void onDatacenterSocketReady(Socket socket) {
        datacenterSockets.add(socket);

        if (datacenterSockets.size() == datacenterIPs.size() - 1) {
            LogUtils.debug(LOG_TAG, "Fully connected to datacenter");
        }
    }

    private void connectToDatacenter() {
        for (AddressConfig ac : datacenterIPs) {
            if (!addressConfig.equals(ac)) {
                executorService.submit(new ConnectToDatacenterRunnable(ac));
            }
        }
    }

    synchronized private void onReplicaSocketReady(Socket socket) {
        replicaSockets.add(socket);

        if (replicaSockets.size() == replicasIPs.size() - 1) {
            LogUtils.debug(LOG_TAG, "Fully connected to replicas");
        }
    }

    private void connectToReplicas() {
        for (AddressConfig ac : replicasIPs) {
            if (!addressConfig.equals(ac)) {
                executorService.submit(new ConnectToReplicaRunnable(ac));
            }
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

    private void handleServerMsg(ServerMsg msg) {
        // TODO
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

            try {
                inputStream = new ObjectInputStream(client.getInputStream());
                TransportObject msg = (TransportObject) inputStream.readObject();
                handleTransportObject(msg);
            } catch (Exception e) {
                LogUtils.error(LOG_TAG, "Something went wrong in handle-client thread", e);
            } finally {
                Utils.closeQuietly(inputStream);
            }
        };

        Utils.startThreadWithName(runnable, "handle-client");
    }

    private void handleTransportObject(TransportObject msg) {
        // TODO
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
                onDatacenterSocketReady(socket);
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Retrying connection to datacenter server " + addressConfig);
                executorService.schedule(this, 1000, TimeUnit.MILLISECONDS);
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
                onReplicaSocketReady(socket);
            } catch (Exception e) {
                LogUtils.debug(LOG_TAG, "Retrying connection to replica server " + addressConfig);
                executorService.schedule(this, 1000, TimeUnit.MILLISECONDS);
            }
        }
    }

    private enum Message {
        _2PC_PREPARE, ACK_2PC_PREPARE, _2PC_ACCEPT, _2PC_COMMIT
    }

    private class ServerMsg implements Serializable {

    }
}
