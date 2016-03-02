package com.lucid.spanner;

import io.atomix.catalyst.transport.Address;

import java.io.Serializable;
import java.util.Map;

/**
 * The commit message object: objects of this class will be serialised and sent from client to server.
 */
public class TransportObject implements Serializable{
    private long txn_id;
    private Map<String, String> writeMap;
    private AddressConfig coordinator;
    private int number_of_leaders;

    private boolean isCoordinator;

    public TransportObject(AddressConfig coord, long id, Map<String, String> map, int num, boolean isC){
        this.txn_id = id;
        writeMap = map;
        this.coordinator = coord;
        this.number_of_leaders = num;
        this.isCoordinator = isC;
    }

    public int getNumber_of_leaders() {
        return number_of_leaders;
    }

    public void setNumber_of_leaders(int number_of_leaders) {
        this.number_of_leaders = number_of_leaders;
    }

    public boolean isCoordinator() {
        return isCoordinator;
    }

    public void setCoordinator(boolean coordinator) {
        isCoordinator = coordinator;
    }

    public long getTxn_id() {
        return txn_id;
    }

    public void setTxn_id(long txn_id) {
        this.txn_id = txn_id;
    }

    public Map<String, String> getWriteMap() {
        return writeMap;
    }

    public void setWriteMap(Map<String, String> writeMap) {
        this.writeMap = writeMap;
    }

    public AddressConfig getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(AddressConfig coordinator) {
        this.coordinator = coordinator;
    }


}
