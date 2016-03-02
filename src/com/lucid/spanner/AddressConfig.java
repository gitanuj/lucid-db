package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

import java.io.Serializable;

public class AddressConfig implements Serializable{

    private String IP;

    private int paxosPort;

    private int clientPort;

    private int serverPort;

    public AddressConfig(String IP, int paxosPort, int clientPort, int serverPort) {
        this.IP = IP;
        this.paxosPort = paxosPort;
        this.clientPort = clientPort;
        this.serverPort = serverPort;
    }

    public String host() {
        return IP;
    }

    public int port() {
        return paxosPort;
    }

    public int getClientPort() {
        return clientPort;
    }

    public int getServerPort() {
        return serverPort;
    }

    public Address toAddress() {
        return new Address(IP, paxosPort);
    }
}
