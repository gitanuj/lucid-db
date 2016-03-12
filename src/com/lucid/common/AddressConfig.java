package com.lucid.common;

import io.atomix.catalyst.transport.Address;

import java.io.Serializable;

public class AddressConfig implements Serializable {

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddressConfig that = (AddressConfig) o;

        if (paxosPort != that.paxosPort) return false;
        if (clientPort != that.clientPort) return false;
        if (serverPort != that.serverPort) return false;
        return IP.equals(that.IP);

    }

    @Override
    public int hashCode() {
        int result = IP.hashCode();
        result = 31 * result + paxosPort;
        result = 31 * result + clientPort;
        result = 31 * result + serverPort;
        return result;
    }

    @Override
    public String toString() {
        return "AddressConfig{" +
                "IP='" + IP + '\'' +
                ", paxosPort=" + paxosPort +
                ", clientPort=" + clientPort +
                ", serverPort=" + serverPort +
                '}';
    }
}
