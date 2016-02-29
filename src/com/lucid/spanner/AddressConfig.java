package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

public class AddressConfig extends Address{

    private int clientPort;
    private int serverPort;

    public AddressConfig(String IP, int paxosPort, int clientPort, int serverPort) {
        super(IP, paxosPort);
        this.clientPort = clientPort;
        this.serverPort = serverPort;
    }

    public int getClientPort() {
        return clientPort;
    }

    public void setClientPort(int clientPort) {
        this.clientPort = clientPort;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }
}
