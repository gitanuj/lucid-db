package com.lucid.rc;

import com.lucid.common.AddressConfig;

import java.io.Serializable;
import java.util.Map;

class ServerMsg implements Serializable {

    enum Message {
        _2PC_PREPARE, ACK_2PC_PREPARE, _2PC_ACCEPT, _2PC_COMMIT
    }

    private long txn_id;

    private Message message;

    private String key;

    private Map<String, String> map;

    private AddressConfig coordinator;

    public ServerMsg(Message message, ServerMsg serverMsg) {
        this(message, serverMsg.getKey(), serverMsg.getMap(), serverMsg.getTxn_id(), serverMsg.getCoordinator());
    }

    public ServerMsg(Message message, String key, Map<String, String> map, long txn_id, AddressConfig coordinator) {
        this.message = message;
        this.key = key;
        this.map = map;
        this.txn_id = txn_id;
        this.coordinator = coordinator;
    }

    public long getTxn_id() {
        return txn_id;
    }

    public Message getMessage() {
        return message;
    }

    public String getKey() {
        return key;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public AddressConfig getCoordinator() {
        return coordinator;
    }

    @Override
    public String toString() {
        return "ServerMsg{" +
                "txn_id=" + txn_id +
                ", message=" + message +
                ", key='" + key + '\'' +
                ", map=" + map +
                ", coordinator=" + coordinator +
                '}';
    }
}
