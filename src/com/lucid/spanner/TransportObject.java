package com.lucid.spanner;

import java.io.Serializable;

/**
 * Objects of this class will be serialised and sent from client to server.
 */
public class TransportObject implements Serializable{
    private long txn_id;
    private String key;
    private String value;

    public TransportObject(long id, String key, String value){
        this.txn_id = id;
        this.key = key;
        this.value = value;
    }
    public long getTxn_id() {
        return txn_id;
    }

    public void setTxn_id(long txn_id) {
        this.txn_id = txn_id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
